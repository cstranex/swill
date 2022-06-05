import logging
import asyncio
import contextvars
import os
import sys
import typing as t
from copy import deepcopy
from functools import cached_property

from ._response import Response, current_response
from .asgi import AsgiApplication
from ._connection import Connection
from ._request import Request, RequestType, _SwillRequestHandler, current_request, \
    RequestReference, AnyRequest
from ._exceptions import Error, HandlerNotFound, SwillRequestCancelled
from ._protocol import ResponseType, EncapsulatedRequest
from ._serialize import deserialize_encapsulated_request, serialize_message, serialize_response
from . import _handlers as _swill_handlers
from ._types import Handler
from .logging import create_logger
from ._helpers import closing_response, get_root_path
from .config import Config


current_app = contextvars.ContextVar('current_app')

logger = logging.getLogger(__name__)


class Swill:

    default_config = {
        'swill': {
            'introspection': True,
        }
    }

    config: Config = None
    asgi_app = None
    _handlers: t.Dict[str, _SwillRequestHandler] = {}
    _exception_handlers: t.Dict[t.Type[BaseException], t.Callable] = {
        BaseException: _swill_handlers.handle_catch_all,
        HandlerNotFound: _swill_handlers.handle_not_found,
        Error: _swill_handlers.handle_error,
    }
    _error_code_handlers: t.Dict[int, t.Callable] = {}
    _lifecycle_handlers: t.Dict[str, t.List[t.Callable]] = {
        'before_connection': [],
        'after_accept': [],
        'before_request': [],
        'before_request_metadata': [],
        'before_request_message': [],
        'before_request_data': [],
        'before_leading_metadata': [],
        'before_response_message': [],
        'before_trailing_metadata': [],
        'after_request': [],
        'after_connection': [],
    }

    def __init__(
        self,
        import_name: str,
        path='/ws',
        debug: bool = None,
        root_path: str = None
    ):
        self.config = self.load_config(root_path or get_root_path(import_name))
        if debug is not None:
            self.config['swill.debug'] = debug
        self.import_name = import_name
        self.debug = debug
        self.path = path
        current_app.set(self)

        # Add swill handlers
        if self.config.get('swill.introspection'):
            self.add_handler(_swill_handlers.introspect(self), 'swill.introspect')

    def load_config(self, path: str):
        return Config(path, deepcopy(self.default_config))

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

    def add_lifecycle_handler(self, name: str, callback: t.Callable):
        if name not in self._lifecycle_handlers:
            raise ValueError(f"{name} is not a lifecycle")

        self._lifecycle_handlers[name].append(callback)

    async def _call_lifecycle_handlers(self, name: str, *args: t.Any):
        if name not in self._lifecycle_handlers:
            raise ValueError(f"{name} is not a lifecycle")

        if handlers := self._lifecycle_handlers[name]:
            logger.debug('Running %s lifecycle handlers for: %s', len(handlers), name)

            for handler in handlers:
                await handler(*args)

    def add_handler(self, f: Handler, handler_name: str = None):
        """Add a new RPC handler."""
        handler_name = f.__name__ if not handler_name else handler_name
        self._handlers[handler_name] = _swill_handlers.create_handler(f)

    def handle(self, name: str = None) -> t.Callable[[Handler], t.Any]:
        """Handle an RPC request. If name is not given then the function name will be used instead"""
        def wrapper(f: Handler):
            self.add_handler(f, name)
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
            await self._call_lifecycle_handlers('before_request_data', request, message)
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
            await self._call_lifecycle_handlers(
                'before_request', request, response, encapsulated_message
            )
            await request.process_message(encapsulated_message)
            if handler.uses_response:
                handler_coro = handler.func(request, response)
            else:
                handler_coro = handler.func(request)

            if not handler.response_streams:
                if isinstance(handler_coro, t.AsyncGenerator):
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
            # Log that we couldn't find this handler
            logger.info('no handler for `%s` was found', encapsulated_message.rpc)
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
        coro: t.Awaitable,
        request: AnyRequest,
        response: Response,
        connection: Connection,
        message_type: t.Any
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
        generator: t.AsyncGenerator,
        request: AnyRequest,
        response: Response,
        connection: Connection,
        message_type: t.Any
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

    async def __call__(self, scope: dict, receive: t.Callable, send: t.Callable):
        if not self.asgi_app:
            self.asgi_app = AsgiApplication(
                [self.path], self._call_lifecycle_handlers, self._handle_request
            )

        return await self.asgi_app(scope, receive, send)

    def run(self):
        pass
