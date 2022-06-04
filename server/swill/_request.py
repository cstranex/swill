import abc
import contextvars
import typing as t
import warnings
from msgspec import Struct

from ._helpers import StreamingQueue
from ._protocol import EncapsulatedRequest, RequestType
from ._serialize import deserialize_message
from ._exceptions import Error, SwillRequestError
from ._types import Metadata, ContextVarType

RequestParameters = t.TypeVar('RequestParameters')
RequestReference = t.NamedTuple('RequestReference', rpc=str, seq=int)


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
            message = t.cast(RequestParameters, deserialize_message(message, self._message_type))
            await self._swill._call_lifecycle_handlers('before_request_message', self, message)
            self._data = message
        elif message.type == RequestType.CANCEL:
            self.cancel()
        else:
            raise SwillRequestError("Request cannot process type: %s", message.type)

    def __repr__(self):
        return f'<Single Request: {self.reference}>'


class StreamingRequest(BaseRequest, t.Generic[RequestParameters]):

    ended = False
    opening_request = True

    def __init__(self, swill, reference: RequestReference, metadata: t.Optional[Metadata], message_type: t.Type):
        self._queue = StreamingQueue(
            name=str(reference)
        )
        super().__init__(swill, reference, metadata, message_type)

    async def process_message(self, encapsulated_message: EncapsulatedRequest):
        """Process the message by deserializing it and add it to a queue for processing"""
        was_opening_request = self.opening_request
        self.opening_request = False

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
            if not was_opening_request:
                raise SwillRequestError("Metadata can only be sent with the fist request")
            self._metadata = encapsulated_message.metadata
            return

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
    request_message_type: t.Type
    response_message_type: t.Type
    request_streams: bool
    response_streams: bool
    uses_response: bool


AnyRequest = t.Union[Request, StreamingRequest]

current_request = t.cast(ContextVarType[AnyRequest], contextvars.ContextVar('current_request'))
