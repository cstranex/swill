import abc
import asyncio
import contextvars
import typing as t
import warnings
from asyncio import Event, Queue
from msgspec import Struct

from ._protocol import EncapsulatedRequest, RequestType
from ._serialize import deserialize_message
from ._exceptions import Error, SwillException, SwillRequestCancelled
from ._types import Metadata, ContextVarType

RequestParameters = t.TypeVar('RequestParameters')
RequestReference = t.NamedTuple('RequestReference', rpc=str, seq=int)


class _StreamingQueue:
    """A queue of incoming messages for a StreamingRequest to process"""

    def __init__(self, name: str):
        self.name = name
        self._queue = Queue()
        self._close_event = Event()
        self._cancel_event = Event()
        self._tasks = []

    def add(self, message):
        """Add message to the queue"""
        self._queue.put_nowait(message)

    def close(self):
        """Close the streaming queue.
        This will cause the generator to raise StopAsyncIteration."""
        self._close_event.set()

    def cancel(self):
        """Cancel the streaming queue

        This will cause the generator to raise SwillRequestCancelled"""
        self._cancel_event.set()

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._close_event.is_set():
            raise StopAsyncIteration()

        _get_item = asyncio.create_task(
            self._queue.get(),
            name=f'swill-queue-{self.name}'
        )
        _close_event = asyncio.create_task(
            self._close_event.wait(),
            name=f'swill-queue-{self.name}'
        )
        _cancel_event = asyncio.create_task(
            self._cancel_event.wait(),
            name=f'swill-queue-{self.name}'
        )
        self._tasks = [_get_item, _close_event, _cancel_event]

        done, pending = await asyncio.wait(self._tasks, return_when=asyncio.FIRST_COMPLETED)

        for task in pending:
            task.cancel()
            self._tasks = []

        if _cancel_event in done:
            raise SwillRequestCancelled()

        if _get_item in done:
            return _get_item.result()

        if _close_event in done:
            raise StopAsyncIteration()

        raise StopAsyncIteration()


class BaseRequest(abc.ABC, t.Generic[RequestParameters]):
    """The base request object that runs for the lifetime of the RPC request.

    Contains various information about the request from the first message received message such
    as the sequence id and metadata. It also exposes the Response that can be used to send
    leading and trailing metdata
    """

    _data = None
    _cancelled = False

    def __init__(self, swill, reference: RequestReference, metadata: t.Optional[Metadata], message_type: t.Type[Struct]):
        self._swill = swill
        self._metadata = metadata
        self._reference = reference
        self._message_type = message_type

    async def process_message(self, message: EncapsulatedRequest):
        """Process the incoming message"""
        raise NotImplementedError

    def abort(self, *, code: int = 500, message: str = ''):
        """Raise an Error exception"""
        raise Error(code=code, message=message)

    def cancel(self):
        """Cancel the running Request"""
        self._cancelled = True

    @property
    def reference(self):
        """Return the RequestReference for this request"""
        return self._reference

    @property
    def seq(self):
        """The sequence id for this request"""
        return self._reference.seq

    @property
    def data(self) -> RequestParameters:
        return self._data

    @property
    def metadata(self):
        """Return the metadata sent by the client"""
        return self._metadata or {}

    @property
    def cancelled(self):
        """Indicates if the request has been cancelled"""
        return self._cancelled

    def __repr__(self):
        return f'<Request: {self.reference}>'


class Request(BaseRequest, t.Generic[RequestParameters]):
    async def process_message(self, message: EncapsulatedRequest):
        """Set the data attribute by processing the incoming message."""
        if message.type == RequestType.MESSAGE:
            self._data = t.cast(RequestParameters, deserialize_message(message, self._message_type))
        elif message.type == RequestType.CANCEL:
            self.cancel()
        else:
            raise SwillException("Request cannot process type: %s", message.type)

    def __repr__(self):
        return f'<Single Request: {self.reference}>'


class StreamingRequest(BaseRequest, t.Generic[RequestParameters]):

    ended = False
    opening_request = True

    def __init__(self, swill, reference: RequestReference, metadata: t.Optional[Metadata], message_type: t.Type[Struct]):
        self._queue = _StreamingQueue(
            name=str(reference)
        )
        super().__init__(swill, reference, metadata, message_type)

    async def process_message(self, encapsulated_message: EncapsulatedRequest):
        """Process the message by deserializing it and add it to a queue for processing"""

        if encapsulated_message.type == RequestType.CANCEL:
            self.cancel()
            return

        if encapsulated_message.type == RequestType.END_OF_STREAM:
            self.end()
            return

        # If we are ended then don't process any further messages
        if self.ended:
            warnings.warn(
                "Request ended but process_message received another message",
                UserWarning
            )
            return

        if encapsulated_message.type == RequestType.METADATA:
            if not self.opening_request:
                raise SwillException("Metadata can only be sent with the fist request")
            self._metadata = encapsulated_message.metadata
            return

        self.opening_request = False
        self._data = message = deserialize_message(encapsulated_message, self._message_type)
        await self._swill._call_lifecycle_handlers('before_request_message', self, message)
        self._queue.add(message)

    def end(self):
        """Closes the request."""
        self._queue.close()
        self.ended = True

    def cancel(self):
        """Cancel the running Request. Usually this is called when the client sends
        a CANCELLED message."""
        super().cancel()
        self._queue.cancel()

    @property
    def data(self) -> t.AsyncGenerator[RequestParameters, None]:
        return self._queue

    def __repr__(self):
        return f'<Streaming Request: {self.reference}>'


class _SwillRequestHandler(t.NamedTuple):
    """A reference to an RPC handler"""

    func: t.Callable
    request_type: t.Type[t.Union[Request, StreamingRequest]]
    request_message_type: t.Type[Struct]
    response_message_type: t.Type[Struct]
    request_streams: bool
    response_streams: bool
    uses_response: bool


AnyRequest = t.Union[Request, StreamingRequest]

current_request = t.cast(ContextVarType[AnyRequest], contextvars.ContextVar('current_request'))
