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

    async def get_send_data(self):
        """Wait for data to send out the websocket"""
        return await self._send_queue.get()

    def __getattr__(self, item):
        return getattr(self.__connection, item)


class SwillTestRequest:

    class CloseContext(Exception):
        pass

    def __init__(self, app: "Swill", initial_request: EncapsulatedRequest, connection: Connection):
        self.app = app
        self.request = initial_request
        self.connection = SwillTestConnection(connection)
        current_connection.set(self.connection)

    def __enter__(self):
        self._request_task = asyncio.create_task(
            self.app._handle_request(serialize_message(self.request), self.connection)
        )

    async def receive(self):
        data = await self.connection.get_send_data()
        response_message = _decoder.decode(data)

        message = None

        if response_message.type == ResponseType.MESSAGE:
            response_type = self.app._handlers[self.request.rpc].response_message_type
            message = deserialize_message(response_message, response_type)
        elif response_message.type == ResponseType.ERROR:
            message = deserialize_message(response_message, ErrorMessage)

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

    async def cancel(self):
        await self.send(type=RequestType.CANCEL)

    async def close(self):
        raise self.CloseContext()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._request_task.cancel()
        if exc_type == self.CloseContext:
            return


class SwillTestClient:
    """Swill Test Client that can be used to test your Swill Application.

    Usage:
    async with SwillTestClient(app) as client:
        with client.request(message_args) as request:
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
