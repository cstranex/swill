import asyncio
import typing as t
import msgspec
from swill import ConnectionData, Connection, current_connection
from swill._protocol import RequestType, EncapsulatedRequest, EncapsulatedResponse, ResponseType
from swill._serialize import serialize_message, deserialize_message
from swill._types import Metadata, ErrorMessage

_decoder = msgspec.msgpack.Decoder(type=EncapsulatedResponse)


class SwillTestConnection:
    """A test connection"""

    def __init__(self, connection: Connection):
        self.__connection = connection
        self._send_queue = asyncio.Queue()

    async def send(self, data):
        """Queue data out of the websocket"""
        await self._send_queue.put(data)

    def has_send_data(self):
        return self._send_queue.qsize()

    async def get_send_data(self):
        """Wait for data to send out the websocket"""
        return await self._send_queue.get()

    def __getattr__(self, item):
        return getattr(self.__connection, item)


class SwillTestRequest:

    _request_ended = False

    class CloseContext(Exception):
        pass

    def __init__(self, app: "Swill", initial_request: EncapsulatedRequest, connection: Connection):
        self.app = app
        self.request = initial_request
        self.connection = SwillTestConnection(connection)
        current_connection.set(self.connection)

    async def __aenter__(self):
        # Handle the initial request. For streaming requests this will continue to run
        # indefinitely, so we can't await here. Instead, wrap it in a task.
        self._request_task = asyncio.create_task(
            self.app._handle_request(serialize_message(self.request), self.connection)
        )
        self._request_task.add_done_callback(self._close_request)
        # Give the handler a chance to run by waiting
        # Not sure if there is a better way to do this
        await asyncio.sleep(0.01)
        return self

    async def receive(self):
        data = await self.connection.get_send_data()
        response_message = _decoder.decode(data)

        message = None

        if response_message.type == ResponseType.MESSAGE:
            response_type = self.app._handlers[self.request.rpc].response_message_type
            message = deserialize_message(response_message, response_type)
        elif response_message.type == ResponseType.ERROR:
            message = deserialize_message(response_message, ErrorMessage)
            self._request_ended = True
        elif response_message.type == ResponseType.END_OF_STREAM:
            self._request_ended = True

        # We need to decode the data
        return {
            'seq': response_message.seq,
            'type': response_message.type,
            'data': message,
            'leading_metadata': response_message.leading_metadata,
            'trailing_metadata': response_message.trailing_metadata,
        }

    async def send(
        self,
        data: t.Any = None,
        metadata: Metadata = None,
        type: RequestType = RequestType.MESSAGE,
    ):
        """Send the given message to the handler"""
        request = EncapsulatedRequest(
            rpc=self.request.rpc,
            type=type,
            data=data,
            metadata=metadata,
            seq=self.request.seq
        )
        await self.app._handle_request(serialize_message(request), self.connection)

    async def end_of_stream(self):
        await self.send(type=RequestType.END_OF_STREAM)
        self._request_ended = True

    async def cancel(self):
        await self.send(type=RequestType.CANCEL)
        self._request_ended = True

    def _close_request(self):
        raise self.CloseContext()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if not self._request_ended:
            await self.cancel()

        close_fut = asyncio.ensure_future(self._request_task)
        try:
            await asyncio.wait_for(close_fut, timeout=30)
        except asyncio.TimeoutError:
            if not self._request_task.done() and not self._request_task.cancelled():
                self._request_task.cancel()

        if self._request_task.exception():
            raise self._request_task.exception()

        return not exc_type or exc_type == self.CloseContext

class SwillTestClient:
    """Swill Test Client that can be used to test your Swill Application.

    Usage:
    async with SwillTestClient(app) as client:
        async with client.request(message_args) as request:
            await request.receive()
            await request.send(message_args_again)
            await request.close()
    """

    token = None
    loop = None
    connection: Connection = None
    _current_sequence_id = 0

    def __init__(self, app: "Swill"):
        self.app = app

    async def __aenter__(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        connection_data = ConnectionData({
            'subprotocols': ConnectionData.subprotocol
        })
        await self.app._call_lifecycle_handlers('before_connection', connection_data)

        # Create a new Connection object
        self.connection = Connection(
            self._send,
            connection_data
        )

        await self.app._call_lifecycle_handlers('after_accept', self.connection)
        self.token = current_connection.set(Connection)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            await self.app._call_lifecycle_handlers('after_connection', self.connection)
        if self.token:
            current_connection.reset(self.token)

    async def _send(self):
        """Sends data out of the connection object"""
        raise NotImplemented

    def request(
        self,
        rpc: str,
        data: t.Any,
        metadata: Metadata = None,
        type: RequestType = RequestType.MESSAGE,
        seq: int = None
    ):
        """Send a message. Takes care of generating a sequence id unless passed"""
        if not seq:
            self._current_sequence_id += 1
            seq = self._current_sequence_id

        req = EncapsulatedRequest(
            seq=seq,
            rpc=rpc,
            type=type,
            data=data,
            metadata=metadata
        )

        return SwillTestRequest(self.app, req, self.connection)
