import typing as t
import contextvars
import uuid
from asyncio import Queue
from werkzeug.sansio.request import Request as _SansIORequest
from werkzeug.sansio.response import Response as _SansIOResponse
from werkzeug.datastructures import Headers as _Headers
from ._exceptions import CloseConnection
from ._types import ContextVarType


class InitialRequest(_SansIORequest):
    def __init__(
        self,
        method: str,
        scheme: str,
        server: t.Optional[t.Tuple[str, t.Optional[int]]],
        root_path: str,
        path: str,
        query_string: bytes,
        headers: _Headers,
        remote_addr: t.Optional[str],
        subprotocols: t.List[str],
    ) -> None:
        super().__init__(method, scheme, server, root_path, path, query_string, headers, remote_addr)
        self.subprotocols = subprotocols


class ConnectionData:
    """Connection information established during the WebSocket handshake"""

    max_cookie_size = 4096
    subprotocol = 'swill/1'

    def __init__(self, asgi_scope: dict):
        self.request = InitialRequest(
            method='get',
            scheme=asgi_scope.get('scheme', 'ws'),
            server=asgi_scope['server'],
            root_path=asgi_scope['root_path'],
            path=asgi_scope['path'],
            query_string=asgi_scope['query_string'],
            headers=asgi_scope['headers'],
            remote_addr=asgi_scope['client'][0] if 'client' in asgi_scope else None,
            subprotocols=asgi_scope['subprotocols']
        )
        self.response = _SansIOResponse()

    def choose_subprotocol(self):
        """Choose the best matching subprotocol and return it. If one cannot be found raise a CloseConnection error"""
        if self.subprotocol not in self.request.subprotocols:
            raise CloseConnection(code=406, reason='No suitable subprotocol')
        return self.subprotocol

    @property
    def request_headers(self):
        return self.request.headers

    @property
    def response_headers(self):
        return self.response.headers

    def get_asgi_response_headers(self):
        return [(key.encode('utf-8'), value.encode('utf-8')) for key, value in self.response.headers.to_wsgi_list()]


class Connection:
    """A connection object that is active during the lifetime of the WebSocket. It holds connection data with the
    initial HTTP request data and the initial HTTP response data."""

    requests = {}

    def __init__(self, send, connection_data: ConnectionData):
        self._send_queue = Queue()
        self._send = send
        self.id = uuid.uuid4().hex
        self._connection_data = connection_data

    async def send(self, data):
        """Queue data out of the websocket"""
        await self._send_queue.put(data)

    async def get_send_data(self):
        """Wait for data to send out the websocket"""
        return await self._send_queue.get()

    def __getattr__(self, item):
        return getattr(self._connection_data, item)

    def __repr__(self):
        return f'<Connection {self.id}: {self._connection_data.request.remote_addr}>'


current_connection = t.cast(
    ContextVarType[Connection],
    contextvars.ContextVar('current_connection')
)
