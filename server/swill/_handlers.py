"""Swill in-built Handlers"""

import asyncio
import inspect
import traceback
import typing as t
import functools
from collections.abc import AsyncIterator

from . import StreamingRequest, SwillValidationError
from ._connection import current_connection
from ._response import Response
from ._types import ErrorCode, StreamingResponse, Handler
from ._request import Request, _SwillRequestHandler
from ._protocol import EncapsulatedRequest
from ._introspect import IntrospectedRpc, introspect_handler
from ._exceptions import Error
from ._serialize import serialize_error_response


def wrap_sync_handler(f: Handler) -> Handler:
    """Wrap a synchronous handler"""

    @functools.wraps(f)
    async def handler(*args, **kwargs):
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,  # TODO: Allow user to specify an executor to use
            functools.partial(f, *args, **kwargs),
        )
        return result

    return handler


def create_handler(f: Handler) -> _SwillRequestHandler:
    """Create a SwillRequestHandler for the given handler"""

    can_stream = True
    if not inspect.iscoroutinefunction(f) and not inspect.isasyncgenfunction(f):
        # If we are given a non-async function then we will wrap it in a executor.
        # This will limit it to be non-streaming
        can_stream = False
        f = wrap_sync_handler(f)

    function_types = t.get_type_hints(f, include_extras=True)
    parameter_names = list(inspect.signature(f).parameters.keys())
    response_message_type = function_types.get("return", None)
    request_type = Request
    request_streams = False
    response_streams = False
    uses_response = False

    if (origin := t.get_origin(response_message_type)) and origin == AsyncIterator:
        response_streams = True
        _args = t.get_args(response_message_type)
        response_message_type = _args[0] if _args else None

    if parameter_names:
        _type = function_types.get(parameter_names[0], None)
        if origin := t.get_origin(_type):
            request_type = origin
            request_streams = origin == StreamingRequest
        _args = t.get_args(_type)

        if len(parameter_names) > 1:
            uses_response = function_types[parameter_names[1]] == Response
    else:
        _args = None

    if not can_stream and (request_streams or response_streams):
        raise RuntimeError("Synchronous handlers cannot handle streaming messages")

    message_type = _args[0] if _args else None
    return _SwillRequestHandler(
        func=f,
        request_type=request_type,
        request_streams=request_streams,
        response_streams=response_streams,
        request_message_type=message_type,
        response_message_type=response_message_type,
        uses_response=uses_response,
    )


# Other handlers
def introspect(swill: "Swill"):
    async def introspect_handlers(request: Request) -> StreamingResponse[IntrospectedRpc]:
        ignore_swill = True
        for name, request_handler in t.cast(
            t.List[t.Tuple[str, _SwillRequestHandler]], swill._handlers.items()
        ):
            if name.startswith("swill.") and ignore_swill:
                continue
            yield introspect_handler(name, request_handler)

    return introspect_handlers


# Exception Handlers
async def handle_not_found(exception: BaseException, message: EncapsulatedRequest):
    await current_connection.get().send(
        serialize_error_response(
            code=ErrorCode.NOT_FOUND, seq=message.seq, message=str(exception)
        )
    )


async def handle_catch_all(exception: BaseException, message: EncapsulatedRequest):
    traceback.print_exception(exception.__class__, exception, exception.__traceback__)

    await current_connection.get().send(
        serialize_error_response(
            code=ErrorCode.INTERNAL_ERROR,
            seq=message.seq,
            message="An internal server error occurred",
        )
    )


async def handle_error(exception: Error, message: EncapsulatedRequest):
    traceback.print_exception(exception.__class__, exception, exception.__traceback__)

    await current_connection.get().send(
        serialize_error_response(
            code=exception.code, seq=message.seq, message=str(exception)
        )
    )


async def handle_validation_error(
    exception: SwillValidationError, message: EncapsulatedRequest
):
    await current_connection.get().send(
        serialize_error_response(
            code=ErrorCode.VALIDATION_ERROR,
            seq=message.seq,
            message=str(exception),
            data=exception.data,
        )
    )
