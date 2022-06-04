import logging
import asyncio
import contextvars
import inspect
import os
import sys
import typing
from functools import cached_property
from collections.abc import AsyncIterator

from ._response import Response, current_response
from .asgi import AsgiApplication
from ._connection import Connection
from ._request import Request, StreamingRequest, RequestType, _SwillRequestHandler, current_request, \
    RequestReference, AnyRequest
from ._exceptions import Error, HandlerNotFound, SwillRequestCancelled
from ._protocol import ResponseType, EncapsulatedRequest
from ._serialize import deserialize_encapsulated_request, serialize_message, serialize_response
from . import _handlers as _swill_handlers
from ._types import Handler
from .logging import create_logger
from ._helpers import closing_response


current_app = contextvars.ContextVar('current_app')

logger = logging.getLogger(__name__)


class Swill:
    config = {
        'introspection.enabled': True
    }
    asgi_app = None
    _handlers: typing.Dict[str, _SwillRequestHandler] = {}
    _exception_handlers: typing.Dict[typing.Type[BaseException], typing.Callable] = {
        BaseException: _swill_handlers.handle_catch_all,
        HandlerNotFound: _swill_handlers.handle_not_found,
        Error: _swill_handlers.handle_error,
    }
    _error_code_handlers: typing.Dict[int, typing.Callable] = {}
    _lifecycle_handlers: typing.Dict[str, typing.List[typing.Callable]] = {
        'before_connection': [],
        'after_accept': [],
        'before_request': [],
        'before_request_metadata': [],
        'before_request_data': [],
        'before_request_message': [],
        'before_leading_metadata': [],
        'before_response_message': [],
        'before_trailing_metadata': [],
        'after_request': [],
        'after_connection': [],
    }

    def __init__(self, import_name: str, path='/ws', debug=False):
        self.import_name = import_name
        self.debug = debug
        self.path = path
        current_app.set(self)
        # Add swill handlers
        if self.config.get('introspection.enabled'):
            self._create_handler('swill.introspect', _swill_handlers.introspect(self))

    @property
    def name(self) -> str:
        if self.import_name == "__main__":
            fn = getattr(sys.modules["__main__"], "__file__", None)
            if fn is None:
                return "__main__"
            return os.path.splitext(os.path.basename(fn))[0]
        return self.import_name

    @cached_property
    def logger(self):
        return create_logger(self)

    def add_lifecycle_handler(self, name: str, callback: typing.Callable):
        if name not in self._lifecycle_handlers:
            raise ValueError(f"{name} is not a lifecycle")

        self._lifecycle_handlers[name].append(callback)

    async def _call_lifecycle_handlers(self, name: str, *args: typing.Any):
        if name not in self._lifecycle_handlers:
            raise ValueError(f"{name} is not a lifecycle")

        if handlers := self._lifecycle_handlers[name]:
            logger.debug('Running %s lifecycle handlers for: %s', len(handlers), name)

            for handler in handlers:
                await handler(*args)

    def _create_handler(self, handler_name: str, f: Handler):
        function_types = typing.get_type_hints(f)
        parameter_names = list(inspect.signature(f).parameters.keys())
        response_message_type = function_types.get('return', None)
        request_type = Request
        request_streams = False
        response_streams = False
        uses_response = False

        if (origin := typing.get_origin(response_message_type)) and origin == AsyncIterator:
            response_streams = True
            _args = typing.get_args(response_message_type)
            response_message_type = _args[0] if _args else None

        if parameter_names:
            _type = function_types.get(parameter_names[0], None)
            if origin := typing.get_origin(_type):
                request_type = origin
                request_streams = origin == StreamingRequest
            _args = typing.get_args(_type)

            if len(parameter_names) > 1:
                uses_response = function_types[parameter_names[1]] == Response
        else:
            _args = None

        message_type = _args[0] if _args else None

        self._handlers[handler_name] = _SwillRequestHandler(
            func=f,
            request_type=request_type,
            request_streams=request_streams,
            response_streams=response_streams,
            request_message_type=message_type,
            response_message_type=response_message_type,
            uses_response=uses_response,
        )

    def handle(self, name: str = None) -> typing.Callable[[Handler], typing.Any]:
        """Handle an RPC request. If name is not given then the function name will be used instead"""
        def wrapper(f: Handler):
            handler_name = f.__name__ if not name else name
            self._create_handler(handler_name, f)
            return f
        return wrapper

    async def _handle_exception(self, exception: BaseException, message: EncapsulatedRequest):
        """Run the appropriate exception func for the given exception"""
        if exception.__class__ in self._exception_handlers:
            await self._exception_handlers[exception.__class__](exception, message)
        else:
            await self._exception_handlers[BaseException](exception, message)

    async def _handle_error_code(self, exception: Error, message: EncapsulatedRequest):
        """Look up error codes and see"""
        if exception.code in self._exception_handlers:
            await self._error_code_handlers[exception.code](exception, message)
        else:
            await _swill_handlers.handle_error(exception, message)

    async def _feed_request(
        self, request: Request, response: Response, message: EncapsulatedRequest
    ):
        request_token = current_request.set(request)
        response_token = current_response.set(response)
        try:
            await self._call_lifecycle_handlers(
                'before_request_data',
                request,
                response,
                message
            )
            await request.process_message(message)
        except Exception as e:
            raise e
        finally:
            current_request.reset(request_token)
            current_response.reset(response_token)

    async def _handle_request(self, data: bytes, connection: Connection):
        """Wait for the request processor and capture and handle any exceptions"""

        encapsulated_message = deserialize_encapsulated_request(data)
        if current_task := asyncio.current_task():
            # Improve readability by setting the task name to the rpc to be called
            current_task.set_name(f'func-{encapsulated_message.rpc}')

        request_reference = RequestReference(
            rpc=encapsulated_message.rpc,
            seq=encapsulated_message.seq
        )

        # Send messages to the already open request for this request reference if it
        # exists.
        if original_request := connection.requests.get(request_reference):
            request, response = original_request

            try:
                await self._feed_request(request, response, encapsulated_message)
            except Exception as exception:
                await self._handle_exception(exception, encapsulated_message)
            return

        if encapsulated_message.type in [RequestType.CANCEL, RequestType.END_OF_STREAM]:
            # We received and end or close message, yet we do not have an open reference for this
            # Issue a warning and return
            self.logger.warning(
                'Received %s for %s but that reference is not open.',
                encapsulated_message.type,
                request_reference
            )
            return

        request = None
        response = None
        request_token = None
        response_token = None

        # Start a new request
        try:
            request, handler = self._create_request(encapsulated_message)
            response = Response(self, request)
            connection.requests[request_reference] = (request, response)

            request_token = current_request.set(request)
            await self._call_lifecycle_handlers('before_request', request, response)
            await request.process_message(encapsulated_message)
            if handler.uses_response:
                handler_coro = handler.func(request, response)
            else:
                handler_coro = handler.func(request)

            if not handler.response_streams:
                if isinstance(handler_coro, typing.AsyncGenerator):
                    raise RuntimeError("Received a streaming response for a single response type")
                await self._process_single_response(
                    coro=handler_coro,
                    request=request,
                    response=response,
                    connection=connection,
                    message_type=handler.response_message_type
                )
            else:
                await self._process_streaming_response(
                    generator=handler_coro,
                    request=request,
                    response=response,
                    connection=connection,
                    message_type=handler.response_message_type
                )
        except Exception as exception:
            await self._handle_exception(exception, encapsulated_message)
        finally:

            # Remove the request_reference
            if request_reference in connection.requests:
                del connection.requests[request_reference]

            if request:
                await self._call_lifecycle_handlers('after_request', request)

            if response_token:
                current_response.reset(response_token)

            if request_token:
                current_request.reset(request_token)

    def _create_request(self, encapsulated_message: EncapsulatedRequest):

        if encapsulated_message.rpc not in self._handlers:
            raise HandlerNotFound('No func was found to process this request')

        request_reference = RequestReference(
            rpc=encapsulated_message.rpc,
            seq=encapsulated_message.seq
        )
        handler = self._handlers[encapsulated_message.rpc]
        request = handler.request_type(
            self,
            request_reference,
            encapsulated_message.metadata,
            handler.request_message_type
        )

        return request, handler

    async def _process_single_response(
        self,
        coro: typing.Awaitable,
        request: AnyRequest,
        response: Response,
        connection: Connection,
        message_type: typing.Any
    ):
        try:
            result = await coro
        except SwillRequestCancelled:
            # The handler didn't handle the request cancellation
            pass

        if request.cancelled:
            logger.info('Request cancelled: %s', request)
            return

        await self._call_lifecycle_handlers('before_trailing_metadata', request,
                                            response.trailing_metadata)
        await self._call_lifecycle_handlers('before_response_message', request, result)
        await connection.send(
            serialize_response(
                data=serialize_message(result, message_type),
                seq=request.seq,
                trailing_metadata=response.trailing_metadata,
                leading_metadata=await response.consume_leading_metadata(),
            )
        )

    async def _process_streaming_response(
        self,
        generator: typing.AsyncGenerator,
        request: AnyRequest,
        response: Response,
        connection: Connection,
        message_type: typing.Any
    ):
        """Process a streaming response"""
        # Wrap the generator so that it always closes
        async with closing_response(generator) as responses:
            async for result in responses:
                if request.cancelled:
                    # Let the handler know that we were cancelled
                    await generator.athrow(SwillRequestCancelled)
                    break

                await self._call_lifecycle_handlers(
                    'before_response_message',
                    request,
                    result
                )

                await connection.send(
                    serialize_response(
                        data=serialize_message(result, message_type),
                        seq=request.seq,
                        leading_metadata=await response.consume_leading_metadata()
                    )
                )

        if request.cancelled:
            logger.info('Request cancelled: %s', request)
            return

        await self._call_lifecycle_handlers('before_trailing_metadata', self,
                                            response.trailing_metadata)
        await connection.send(
            serialize_response(
                type=ResponseType.END_OF_STREAM,
                seq=request.seq,
                leading_metadata=await response.consume_leading_metadata(),
                trailing_metadata=response.trailing_metadata
            )
        )

    async def __call__(self, scope: dict, receive: typing.Callable, send: typing.Callable):
        if not self.asgi_app:
            self.asgi_app = AsgiApplication(self, self._call_lifecycle_handlers, self._handle_request)

        return await self.asgi_app(scope, receive, send)

    def run(self):
        pass
