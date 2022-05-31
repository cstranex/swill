import logging
import asyncio
import contextvars
import inspect
import os
import sys
import typing
from functools import cached_property
from collections.abc import AsyncIterator

from .asgi import AsgiApplication
from ._connection import Connection
from ._request import Request, StreamingRequest, RequestType, _SwillRequestHandler
from ._exceptions import Error, HandlerNotFound
from ._protocol import ResponseType, EncapsulatedRequest
from ._serialize import deserialize_encapsulated_request, serialize_message, serialize_response
from . import _handlers as _exception_handlers
from ._types import Handler
from .logging import create_logger


current_app = contextvars.ContextVar('current_app')

logger = logging.getLogger(__name__)


class Swill:
    asgi_app = None
    _handlers: typing.Dict[str, _SwillRequestHandler] = {}
    _exception_handlers: typing.Dict[typing.Type[BaseException], typing.Callable] = {
        BaseException: _exception_handlers.handle_catch_all,
        HandlerNotFound: _exception_handlers.handle_not_found,
        Error: _exception_handlers.handle_error,
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

        if self._lifecycle_handlers[name]:
            logger.debug('Running %s lifecycle handlers for: %s', name)
        else:
            return

        for handler in self._lifecycle_handlers[name]:
            await handler(*args)

    def _create_handler(self, handler_name: str, f: Handler):
        function_types = typing.get_type_hints(f)
        parameter_names = list(inspect.signature(f).parameters.keys())
        response_message_type = function_types.get('return', None)
        request_type = Request
        request_streams = False
        response_streams = False

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
            await _exception_handlers.handle_error(exception, message)

    async def _handle_request(self, data: bytes, connection: Connection):
        """Wait for the request processor and capture and handle any exceptions"""

        encapsulated_message = deserialize_encapsulated_request(data)
        if current_task := asyncio.current_task():
            # Improve readability by setting the task name to the rpc to be called
            current_task.set_name(f'func-{encapsulated_message.rpc}')

        stream_reference = (encapsulated_message.rpc, encapsulated_message.seq)

        # Send messages to the already open request for this stream reference if it
        # exists
        if original_request := connection.streams.get(stream_reference):
            await original_request.process_message(encapsulated_message)
            return

        if encapsulated_message.type == RequestType.CLOSE or encapsulated_message.type == RequestType.END_OF_STREAM:
            # We received and end or close message, yet we do not have an open reference for this
            # Issue a warning and return
            self.logger.warning(
                'Received %s for %s but that reference is not open.',
                encapsulated_message.type,
                stream_reference
            )
            return

        await self._call_lifecycle_handlers('before_request', encapsulated_message)
        request, handler = self._create_request(encapsulated_message, connection)
        await request.process_message(encapsulated_message)

        try:
            await self._process_request(request, handler, connection)
        except asyncio.CancelledError:
            pass
        except Exception as exception:
            await self._handle_exception(exception, encapsulated_message)

        # Remove the stream_reference
        if stream_reference in connection.streams:
            del connection.streams[stream_reference]

        await self._call_lifecycle_handlers('after_request', request)

    def _create_request(self, encapsulated_message: EncapsulatedRequest, connection: Connection):

        if encapsulated_message.rpc not in self._handlers:
            raise HandlerNotFound('No func was found to process this request')

        stream_reference = (encapsulated_message.rpc, encapsulated_message.seq)
        handler = self._handlers[encapsulated_message.rpc]
        request = handler.request_type(
            self,
            encapsulated_message,
            handler.request_message_type
        )

        if handler.request_streams:
            connection.streams[stream_reference] = request

        return request, handler

    async def _process_request(
            self,
            request: typing.Union[Request, StreamingRequest],
            handler:
            _SwillRequestHandler,
            connection: Connection
    ):
        """Process the request"""

        handler_coro = handler.func(request)

        if not handler.response_streams:
            # Single responses just wait for the func to return data
            result = await handler_coro

            await self._call_lifecycle_handlers('before_trailing_metadata', request, request.trailing_metadata)
            await self._call_lifecycle_handlers('before_response_message', request, result)
            await connection.send(
                serialize_response(
                    data=serialize_message(result, handler.response_message_type),
                    seq=request.seq,
                    trailing_metadata=request.trailing_metadata,
                    leading_metadata=request.get_leading_metadata(),
                )
            )
        else:
            # Streaming responses yield results which get sent until the generator exits
            # If the result does not have __aiter__ then we just end of stream immediately
            streaming = hasattr(handler_coro, '__aiter__')
            if streaming:
                async for result in handler_coro:
                    if request.cancelled:
                        break
                    await self._call_lifecycle_handlers('before_response_message', request, result)
                    await connection.send(
                        serialize_response(
                            data=serialize_message(result, handler.response_message_type),
                            seq=request.seq,
                            leading_metadata=request.get_leading_metadata()
                        )
                    )

            if not request.cancelled:
                await self._call_lifecycle_handlers('before_trailing_metadata', self, request.trailing_metadata)
                await connection.send(
                    serialize_response(
                        type=ResponseType.END_OF_STREAM,
                        seq=request.seq,
                        leading_metadata=request.get_leading_metadata(),
                        trailing_metadata=request.trailing_metadata
                    )
                )

    async def __call__(self, scope: dict, receive: typing.Callable, send: typing.Callable):
        if not self.asgi_app:
            self.asgi_app = AsgiApplication(self, self._call_lifecycle_handlers, self._handle_request)

        return await self.asgi_app(scope, receive, send)

    def run(self):
        pass
