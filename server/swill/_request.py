import abc
import asyncio
import contextvars
from asyncio import Event, Queue
from typing import Callable, NamedTuple, Type, TypeVar, Generic, Union, cast
from msgspec import Struct

from ._protocol import EncapsulatedRequest, RequestType
from ._serialize import deserialize_message
from ._exceptions import Error, SwillException
from ._response import Response

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


class BaseRequest(abc.ABC, Generic[RequestParameters]):
    """The base request object that runs for the lifetime of the RPC request.

    Contains various information about the request from the first message received message such
    as the sequence id and metadata. It also exposes the Response that can be used to send
    leading and trailing metdata
    """

    _data = None
    _leading_metadata = None
    _leading_metadata_sent = False
    _trailing_metadata = None
    _cancelled = False

    def __init__(self, swill, first_message: EncapsulatedRequest, request_type: Type[Struct]):
        self._swill = swill
        self._encapsulated_message = first_message
        self._request_type = request_type
        self.response = Response(swill, self)

    async def process_message(self, message: EncapsulatedRequest):
        """Process the incoming message"""
        raise NotImplementedError

    def abort(self, *, code: int = 500, message: str = ''):
        """Raise an Error exception"""
        raise Error(code=code, message=message)

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


class Request(BaseRequest, Generic[RequestParameters]):
    async def process_message(self, message: EncapsulatedRequest):
        """Set the data attribute by processing the incoming message."""
        if message.type == RequestType.MESSAGE:
            self._data = cast(RequestParameters, deserialize_message(message, self._request_type))
        elif message.type == RequestType.CLOSE:
            self._cancelled = True
        else:
            raise SwillException("BaseRequest cannot process type: %s", message.type)


class StreamingRequest(BaseRequest, Generic[RequestParameters]):

    ended = False
    opening_request = True

    def __init__(self, swill, first_message: EncapsulatedRequest, request_type: Type[Struct]):
        self._end_of_stream = first_message.type == RequestType.END_OF_STREAM
        self._queue = _StreamingQueue()
        super().__init__(swill, first_message, request_type)
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


class _SwillRequestHandler(NamedTuple):
    """A reference to an RPC handler"""

    func: Callable
    request_type: Type[Union[BaseRequest, StreamingRequest]]
    request_message_type: Type[Struct]
    response_message_type: Type[Struct]
    request_streams: bool
    response_streams: bool


current_request = contextvars.ContextVar('current_request')
