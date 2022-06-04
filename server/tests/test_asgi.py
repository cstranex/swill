from unittest.mock import ANY

import pytest
from werkzeug.sansio.response import Response as _SansIOResponse

from swill import ConnectionData, CloseConnection, Connection
from swill.asgi import AsgiApplication


def make_default_application(mocker, paths=None):
    if not paths:
        paths = ['/ws']
    lifecycle_handler = mocker.AsyncMock()
    request_handler = mocker.AsyncMock()
    app = AsgiApplication(paths, lifecycle_handler, request_handler)

    return app, lifecycle_handler, request_handler, mocker.AsyncMock(), mocker.AsyncMock()


@pytest.mark.asyncio
async def test_asgi_http_request(mocker):
    """Test making a request to a regular http endpoint"""

    app, lifecycle, request, send, receive = make_default_application(mocker)

    await app({
        'type': 'http',
        'path': '/ws'
    }, receive, send)
    start_call = mocker.call({
        'type': 'http.response.start',
        'status': 404,
        'headers': [(b'Content-Type', b'text/plain')],
    })
    body_call = mocker.call({
        'type': 'http.response.body',
        'body': b'404 Not Found'
    })

    send.assert_has_awaits([start_call, body_call])


@pytest.mark.asyncio
async def test_asgi_invalid_path(mocker):
    app, lifecycle, request, send, receive = make_default_application(mocker)

    await app({
        'type': 'websocket',
        'path': '/invalid'
    }, receive, send)
    start_call = mocker.call({
        'type': 'http.response.start',
        'status': 404,
        'headers': [(b'Content-Type', b'text/plain')],
    })
    body_call = mocker.call({
        'type': 'http.response.body',
        'body': b'404 Not Found'
    })

    send.assert_has_awaits([start_call, body_call])


@pytest.mark.asyncio
async def test_asgi_valid_path(mocker):
    app, lifecycle, request, send, receive = make_default_application(mocker)

    app._websocket_handshake = mocker.AsyncMock(return_value=False)

    await app({
        'type': 'websocket',
        'path': '/ws'
    }, receive, send)
    app._websocket_handshake.assert_awaited_once()


@pytest.mark.asyncio
async def test_asgi_websocket_handshake(mocker):
    app, lifecycle, request, send, receive = make_default_application(mocker)

    class TestConnectionData(ConnectionData):
        def __init__(self, scope):
            pass

        def choose_subprotocol(self):
            return 'swill/1'

        def get_asgi_response_headers(self):
            return [(b'X-Test', b'value')]

    test_connection_data = TestConnectionData({})
    return_mock = mocker.MagicMock(return_value=test_connection_data)

    mocker.patch('swill.asgi.ConnectionData', new=return_mock)

    # Send a websocket.connect event
    scope = {
        'type': 'websocket',
        'asgi': {'spec_version': '2.3', 'version': '3.0'},
        'scheme': 'ws',
        'http_version': '1.1',
        'path': '/ws',
        'raw_path': b'/ws',
        'query_string': b'',
        'root_path': '',
        'headers': [
            (b'host', b'localhost:8000'),
            (b'user-agent', b'Mozilla/5.0'),
            (b'accept', b'*/*'),
            (b'accept-language', b'en;q=0.5'),
            (b'accept-encoding', b'gzip, deflate, br'),
            (b'origin', b'http://localhost:63342'),
            (b'sec-websocket-protocol', b'swill/1'),
            (b'dnt', b'1'), (b'connection', b'keep-alive, Upgrade'),
            (b'cache-control', b'no-cache'),
            (b'upgrade', b'websocket')],
        'client': ('127.0.0.1', 56496),
        'server': ('127.0.0.1', 8000),
        'subprotocols': ['swill/1'],
        'extensions': {'websocket.http.response': {}}
    }

    receive.return_value = {'type': 'websocket.connect'}

    assert await app._websocket_handshake(scope, receive, send) is not False
    return_mock.assert_called_with(scope)
    lifecycle.assert_awaited_with('before_connection', test_connection_data)
    send.assert_awaited_with({
        'type': 'websocket.accept',
        'subprotocol': 'swill/1',
        'headers': [(b'X-Test', b'value')],
    })


@pytest.mark.asyncio
async def test_asgi_websocket_handshake_failure(mocker):
    app, lifecycle, request, send, receive = make_default_application(mocker)

    class TestConnectionData(ConnectionData):
        def __init__(self, scope):
            self.http_response = _SansIOResponse()

        def choose_subprotocol(self):
            raise CloseConnection()

        def get_asgi_response_headers(self):
            return [(b'X-Test', b'value')]

    test_connection_data = TestConnectionData({})
    return_mock = mocker.MagicMock(return_value=test_connection_data)

    mocker.patch('swill.asgi.ConnectionData', new=return_mock)

    # Send a websocket.connect event
    scope = {
        'type': 'websocket',
        'asgi': {'spec_version': '2.3', 'version': '3.0'},
        'scheme': 'ws',
        'http_version': '1.1',
        'path': '/ws',
        'raw_path': b'/ws',
        'query_string': b'',
        'root_path': '',
        'headers': [(b'upgrade', b'websocket')],
        'client': ('127.0.0.1', 56496),
        'server': ('127.0.0.1', 8000),
        'subprotocols': ['swill/1'],
        'extensions': {'websocket.http.response': {}}
    }

    receive.return_value = {'type': 'websocket.connect'}

    assert await app._websocket_handshake(scope, receive, send) is False
    assert send.await_count == 2

    start_call = mocker.call({
        'type': 'http.response.start',
        'status': 403,
        'headers': [(b'X-Test', b'value')],
    })
    body_call = mocker.call({
        'type': 'http.response.body',
        'body': b''
    })

    send.assert_has_awaits([start_call, body_call])


@pytest.mark.asyncio
async def test_asgi_websocket_with_close_connection(mocker):
    app, lifecycle, request, send, receive = make_default_application(mocker)

    scope = {
        'type': 'websocket',
        'asgi': {'spec_version': '2.3', 'version': '3.0'},
        'scheme': 'ws',
        'http_version': '1.1',
        'path': '/ws',
        'raw_path': b'/ws',
        'query_string': b'',
        'root_path': '',
        'headers': [(b'upgrade', b'websocket')],
        'client': ('127.0.0.1', 56496),
        'server': ('127.0.0.1', 8000),
        'subprotocols': ['swill/1'],
        'extensions': {'websocket.http.response': {}}
    }

    async def cause_close(*args):
        raise CloseConnection()

    app._connection_loop = cause_close

    connection_data = ConnectionData(scope)
    app._websocket_handshake = mocker.AsyncMock(return_value=connection_data)

    connection = mocker.MagicMock()
    patched_connection = mocker.patch('swill.asgi.Connection', return_value=connection)

    receive.return_value = {'type': 'websocket.connect'}
    await app(scope, receive, send)
    patched_connection.assert_called_with(ANY, connection_data)
    lifecycle.assert_has_awaits([
        mocker.call('after_accept', connection),
        mocker.call('after_connection', connection),
    ])


@pytest.mark.asyncio
async def test_asgi_connection_loop(mocker):
    app, lifecycle, request, send, receive = make_default_application(mocker)

    connection = mocker.MagicMock(spec=Connection)

    receive = mocker.AsyncMock(side_effect=[
        {
            'type': 'websocket.receive',
            'bytes': b'test'
        },
        {
            'type': 'websocket.disconnect',
            'code': 1003,
        }
    ])

    await app._connection_loop(send, receive, connection)
    app.request_handler.assert_awaited_with(b'test', connection)
