import logging
import asyncio
import typing as t
import typing as t

from ._connection import CloseConnection, Connection, ConnectionData, current_connection

logger = logging.getLogger(__name__)


class AsgiApplication:
    def __init__(self, swill: "Swill", lifecycle_handler: t.Callable, request_handler: t.Callable):
        self.swill = swill
        self.lifecycle_handler = lifecycle_handler
        self.request_handler = request_handler

    async def __call__(self, scope: dict, receive: t.Callable, send: t.Callable):
        """Process an ASGI BaseRequest"""

        # We only deal with websockets. In the future we might also support calling non-streaming requests
        # via HTTP.
        if scope.get('type') != 'websocket' or scope.get('path') != self.swill.path:
            logger.debug('Non-websocket request')
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

        connection_token = None

        # Wait for us to accept the websocket
        while True:
            request_event = await receive()
            if request_event['type'] == 'websocket.connect':
                connection_data = ConnectionData(scope)
                try:
                    subprotocol = connection_data.choose_subprotocol()
                    await self.lifecycle_handler('before_connection', connection_data)
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
            return

        # Helper to abstract the actual sending of data out the websocket
        async def _send(data: bytes):
            await send({
                'type': 'websocket.send',
                'bytes': data
            })

        connection = Connection(_send, connection_data)
        connection_token = current_connection.set(connection)

        try:
            await self.lifecycle_handler('after_accept', connection)
            await self._connection_loop(send, receive, connection)
        except CloseConnection as e:
            # Close the open connection, optionally sending a WebSocket Status Code and reason
            code = e.code if e.code >= 1000 else 1000
            reason = str(e)
            logger.debug('websocket closed. Code: %s. Reason: %s', code, reason)
            await send({
                'type': 'websocket.close',
                'code': code,
                'reason': reason
            })
        except Exception as e:
            if connection_token:
                current_connection.reset(connection_token)
            raise e

        try:
            await self.lifecycle_handler('after_connection', connection)
        except Exception as e:
            if connection_token:
                current_connection.reset(connection_token)
            raise e

    async def _connection_loop(
        self, send: t.Callable, receive: t.Callable, connection: Connection
    ):

        receive_task = asyncio.create_task(receive(), name='receive')
        send_task = asyncio.create_task(connection.get_send_data(), name='send')
        tasks = {receive_task, send_task}

        while True:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                tasks.remove(task)

            if receive_task in done:
                event = receive_task.result()  # From await receive()
                if event['type'] == 'websocket.disconnect':
                    logger.debug('websocket disconnected. Code: %s', event['code'])
                    for task in tasks:
                        # Cancel all remaining tasks for this connection
                        if not task.done() and not task.cancelled():
                            task.cancel()
                    break

                if event['type'] == 'websocket.receive':
                    logger.debug("--> %s", event['bytes'])
                    # Send this to our func for this connection
                    handler_task = asyncio.create_task(
                        self.request_handler(event['bytes'], connection),
                        name='func'
                    )
                    tasks.add(handler_task)
                receive_task = asyncio.create_task(receive(), name='receive')
                tasks.add(receive_task)

            if send_task in done:
                result = send_task.result()
                logger.debug("<-- %s", result)
                await send({
                    'type': 'websocket.send',
                    'bytes': result,
                })
                send_task = asyncio.create_task(connection.get_send_data(), name='send')
                tasks.add(send_task)
