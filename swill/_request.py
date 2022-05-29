import asyncio
from asyncio import Event, Queue
from typing import Callable, NamedTuple, Type, TypeVar, Generic, Union, cast
from msgspec import Struct

from ._types import Metadata
from . import EncapsulatedRequest, RequestType, ResponseType
from ._serialize import deserialize_message, serialize_response
from ._connection import current_connection
from ._exceptions import Error, SwillException

RequestParameters = TypeVar('RequestParameters')


class _StreamingQueue:

    def __init__(self):
        self._queue = Queue()
        self._close_event = Event()
        self._get_item = None
        self._end_of_stream = None

    def add(self, message):
        self._queue.put_nowait(message)

    def close(self):
        self._close_event.set()

    def __aiter__(self):
        return self

    async def __anext__(self):
        self._get_item = asyncio.create_task(self._queue.get())
        self._end_of_stream = asyncio.create_task(self._close_event.wait())
        done, pending = await asyncio.wait([self._get_item, self._end_of_stream], return_when=asyncio.FIRST_COMPLETED)
        if self._get_item in done:
            return self._get_item.result()
        if self._end_of_stream in done:
            raise StopAsyncIteration()
        for task in pending:
            task.cancel()

    def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._get_item and not self._get_item.cancelled():
            self._get_item.cancel()

        if self._end_of_stream and not self._end_of_stream.cancelled():
            self._end_of_stream.cancel()


class Request(Generic[RequestParameters]):

    _data = None
    _leading_metadata = None
    _leading_metadata_sent = False
    _trailing_metadata = None
    _cancelled = False

    def __init__(self, swill, encapsulated_message: EncapsulatedRequest, request_type: Type[Struct]):
        self._swill = swill
        self._encapsulated_message = encapsulated_message
        self._request_type = request_type

    async def process_message(self, message: EncapsulatedRequest):
        if message.type == RequestType.MESSAGE:
            self._data = cast(RequestParameters, deserialize_message(message, self._request_type))
        elif message.type == RequestType.CLOSE:
            self._cancelled = True
        else:
            raise SwillException("Request cannot process type: %s", message.type)

    def abort(self, *, code: int = 500, message: str = ''):
        """Raise an Error exception"""
        raise Error(code=code, message=message)

    async def send_leading_metadata(self):
        await self._swill._call_lifecycle_handlers('before_leading_metadata', self, self._leading_metadata)
        await current_connection.get().send(serialize_response(
            seq=self.seq,
            type=ResponseType.METADATA,
            leading_metadata=self._leading_metadata
        ))
        self._leading_metadata_sent = True

    async def set_leading_metadata(self, metadata: Metadata, send_immediately=True):
        """Set leading metadata for this request. This can only be set once"""

        if self.leading_metadata_sent:
            raise ValueError("Metadata has already been sent for this request")

        self._leading_metadata = metadata
        if send_immediately:
            await self.send_leading_metadata()

    def set_trailing_metadata(self, metadata: Metadata):
        self._trailing_metadata = metadata

    def get_leading_metadata(self):
        if self._leading_metadata_sent:
            return
        else:
            self._leading_metadata_sent = True
            return self._leading_metadata

    @property
    def trailing_metadata(self):
        """Return trailing metadata if there is any or return None"""
        return self._trailing_metadata

    @property
    def leading_metadata_sent(self):
        return self._leading_metadata_sent

    @property
    def leading_metadata(self):
        return self._leading_metadata

    @property
    def seq(self):
        return self._encapsulated_message.seq

    @property
    def message(self):
        return self._encapsulated_message

    @property
    def data(self):
        return self._data

    @property
    def metadata(self):
        return self._encapsulated_message.metadata or {}

    @property
    def cancelled(self):
        return self._cancelled


class StreamingRequest(Request, Generic[RequestParameters]):

    ended = False
    opening_request = True

    def __init__(self, swill, encapsulated_message: EncapsulatedRequest, request_type: Type[Struct]):
        self._end_of_stream = encapsulated_message.type == RequestType.END_OF_STREAM
        self._queue = _StreamingQueue()
        super().__init__(swill, encapsulated_message, request_type)
        self.opening_request = False

    async def process_message(self, encapsulated_message: EncapsulatedRequest, request_type: Type[Struct] = None):
        """Process the message by deserializing it and add it to a queue for processing"""
        await self._swill._call_lifecycle_handlers('before_request_data', self, encapsulated_message)

        if self.ended:
            return

        if encapsulated_message.type == RequestType.CLOSE:
            self._cancelled = True
            return

        if encapsulated_message.type == RequestType.END_OF_STREAM:
            self.end()

        if encapsulated_message.type == RequestType.METADATA:
            if not self.opening_request:
                raise SwillException("Metadata can only be sent with the fist request")
            self._encapsulated_message.metadata = encapsulated_message.metadata

        self._data = message = deserialize_message(encapsulated_message, self._request_type)
        await self._swill._call_lifecycle_handlers('before_request_message', self, message)
        self._queue.add(message)

    def end(self, client_initiated=False):
        self._queue.close()
        self.ended = True
        if not client_initiated:
            self._end_of_stream = True

    def __aiter__(self):
        return self._queue.__aiter__()


class SwillRequestHandler(NamedTuple):
    func: Callable
    request_type: Type[Union[Request, StreamingRequest]]
    request_message_type: Type[Struct]
    response_message_type: Type[Struct]
    request_streams: bool
    response_streams: bool
