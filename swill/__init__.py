import asyncio
import typing
import inspect
from collections.abc import AsyncIterator

from ._types import ErrorCode, Handler, StreamingResponse
from ._protocol import EncapsulatedRequest, RequestType, ResponseType
from ._request import Request, StreamingRequest, SwillRequestHandler
from ._connection import Connection, ConnectionData, current_connection
from . import _handlers as _exception_handlers
from ._exceptions import CloseConnection, HandlerNotFound, Error

from ._serialize import (
    deserialize_encapsulated_request, deserialize_message, serialize_response,
    serialize_message, serialize_error_response
)


class Swill:
    _handlers: typing.Dict[str, SwillRequestHandler] = {}
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

    def __init__(self, path='/ws'):
        self.path = path

    def add_lifecycle_handler(self, name: str, callback: typing.Callable):
        if name not in self._lifecycle_handlers:
            raise ValueError(f"{name} is not a lifecycle")

        self._lifecycle_handlers[name].append(callback)

    async def _call_lifecycle_handlers(self, name: str, *args: typing.Any):
        if name not in self._lifecycle_handlers:
            raise ValueError(f"{name} is not a lifecycle")

        for handler in self._lifecycle_handlers[name]:
            await handler(*args)

    def handle(self, name: str = None) -> typing.Callable[[Handler], typing.Any]:
        """Handle an RPC request. If name is not given then the function name will be used instead"""

        def wrapper(f: Handler):
            handler_name = f.__name__ if not name else name
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

            self._handlers[handler_name] = SwillRequestHandler(
                func=f,
                request_type=request_type,
                request_streams=request_streams,
                response_streams=response_streams,
                request_message_type=message_type,
                response_message_type=response_message_type,
            )

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
            SwillRequestHandler,
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

    async def asgi_app(self, scope: dict, receive: typing.Callable, send: typing.Callable):
        """The ASGI Application"""

        # We only deal with websockets. In the future we might also support calling non-streaming requests
        # via HTTP.
        if scope.get('type') != 'websocket' or scope.get('path') != self.path:
            await send({
                'type': 'http.response.start',
                'status': 404,
                'headers': [(b'Content-Type', b'text/plain')],
            })
            await send({
                'type': 'http.response.body',
                'body': b'404 Not Found'
            })
            return

        # Wait for us to accept the websocket
        while True:
            request_event = await receive()
            if request_event['type'] == 'websocket.connect':
                connection_data = ConnectionData(scope)
                try:
                    subprotocol = connection_data.choose_subprotocol()
                    await self._call_lifecycle_handlers('before_connection', connection_data)
                except CloseConnection as e:
                    # Close the connection
                    if e.code < 1000:
                        connection_data.response.status_code = e.code
                    elif not connection_data.response.status_code:
                        connection_data.response.status_code = 403  # Default return code

                    await send({
                        'type': 'http.response.start',
                        'status': connection_data.response.status_code,
                        'headers': connection_data.get_asgi_response_headers(),
                    })
                    await send({
                        'type': 'http.response.body',
                        'body': str(e).encode('utf-8')
                    })
                    return
                await send({
                    'type': 'websocket.accept',
                    'subprotocol': subprotocol,
                    'headers': connection_data.get_asgi_response_headers(),
                })
                break
            elif request_event['type'] == 'websocket.disconnect':
                return

        # Helper to abstract the actual sending of data out the websocket
        async def _send(data: bytes):
            await send({
                'type': 'websocket.send',
                'bytes': data
            })

        connection = Connection(_send, connection_data)
        current_connection.set(connection)
        await self._call_lifecycle_handlers('after_accept', connection)

        receive_task = asyncio.create_task(receive(), name='receive')
        send_task = asyncio.create_task(connection.get_send_data(), name='send')
        tasks = {receive_task, send_task}

        try:
            while True:
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                [tasks.remove(task) for task in done]

                if receive_task in done:
                    event = receive_task.result()  # From await receive()
                    if event['type'] == 'websocket.disconnect':
                        for task in tasks:
                            # Cancel all remaining tasks for this connection
                            if not task.done() and not task.cancelled():
                                task.cancel()
                        break
                    elif event['type'] == 'websocket.receive':
                        # Send this to our func for this connection
                        # print('-->', event['bytes'])
                        handler_task = asyncio.create_task(
                            self._handle_request(event['bytes'], connection),
                            name='func'
                        )
                        tasks.add(handler_task)
                    receive_task = asyncio.create_task(receive(), name='receive')
                    tasks.add(receive_task)
                if send_task in done:
                    result = send_task.result()
                    # print('<--', result)
                    await send({
                        'type': 'websocket.send',
                        'bytes': result,
                    })
                    send_task = asyncio.create_task(connection.get_send_data(), name='send')
                    tasks.add(send_task)
        except CloseConnection as e:
            # Close the open connection, optionally sending a WebSocket Status Code and reason
            await send({
                'type': 'websocket.close',
                'code': e.code if e.code >= 1000 else 1000,
                'reason': str(e)
            })
            return

        await self._call_lifecycle_handlers('after_connection', connection)

    async def __call__(self, scope: dict, receive: typing.Callable, send: typing.Callable):
        return await self.asgi_app(scope, receive, send)

    def run(self):
        pass
