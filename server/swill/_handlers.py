"""Swill in-built Handlers"""

import traceback
import typing as t

from ._connection import current_connection
from ._types import ErrorCode, StreamingResponse
from ._request import Request, _SwillRequestHandler
from ._protocol import EncapsulatedRequest
from ._introspect import IntrospectedRpc, introspect_handler
from ._exceptions import Error
from ._serialize import serialize_error_response


# Other handlers
def introspect(swill: "Swill"):

    async def introspect_handlers(request: Request) -> StreamingResponse[IntrospectedRpc]:
        ignore_swill = True
        for name, request_handler in t.cast(t.List[t.Tuple[str, _SwillRequestHandler]], swill._handlers.items()):
            if name.startswith('swill.') and ignore_swill:
                continue
            yield introspect_handler(name, request_handler)

    return introspect_handlers


# Exception Handlers
async def handle_not_found(exception: BaseException, message: EncapsulatedRequest):
    traceback.print_exception(
        exception.__class__,
        exception,
        exception.__traceback__
    )

    await current_connection.get().send(
        serialize_error_response(
            code=ErrorCode.NOT_FOUND,
            seq=message.seq,
            message=str(exception)
        )
    )


async def handle_catch_all(exception: BaseException, message: EncapsulatedRequest):
    traceback.print_exception(
        exception.__class__,
        exception,
        exception.__traceback__
    )

    await current_connection.get().send(
        serialize_error_response(
            code=ErrorCode.INTERNAL_ERROR,
            seq=message.seq,
            message='An internal server error occurred'
        )
    )


async def handle_error(exception: Error, message: EncapsulatedRequest):
    traceback.print_exception(
        exception.__class__,
        exception,
        exception.__traceback__
    )

    await current_connection.get().send(
        serialize_error_response(
            code=exception.code,
            seq=message.seq,
            message=str(exception)
        )
    )
