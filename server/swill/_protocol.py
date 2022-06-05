import enum
from typing import Optional

from msgspec import Raw, Struct

from swill._types import Metadata


class ResponseType(enum.IntEnum):
    # A standard message response that may or may not also contain
    # metadata.
    MESSAGE = 0

    # Notify the client that no more messages are expected to be sent.
    # This may also contain trailing metadata
    END_OF_STREAM = 1

    # Used for streaming responses to send leading metadata before the
    # first message
    METADATA = 2

    # Used to send an exception to the client. Assumes END_OF_STREAM
    # for streaming responses
    ERROR = 3


class RequestType(enum.IntEnum):
    # A standard message request that may or may not contain metadata.
    MESSAGE = 0

    # Notify the server that no more messages are expected to be sent.
    END_OF_STREAM = 1

    # Used for streaming requests to send initial metadata before the
    # first message
    METADATA = 2

    # Terminate a streaming response from the client. Equivalent to
    # the server sending an END_OF_STREAM message.
    CANCEL = 3


class EncapsulatedMessage(Struct, array_like=True, omit_defaults=True):
    # Incrementing sequence id for messages. Messages with the same sequence id should be considered related.
    # ie: In a streaming request/response each message sent will use the same sequence id.
    # A sequence id of 0 is reserved.
    seq: int
    data: Raw


class EncapsulatedResponse(EncapsulatedMessage):
    type: ResponseType = ResponseType.MESSAGE

    # leading_metadata can only be sent once and may be sent on its own or with the first data response
    leading_metadata: Optional[Metadata] = None

    # trailing_metadata can only be sent with the end_of_stream message
    # for streaming responses or with the response for non-streaming
    # responses.
    trailing_metadata: Optional[Metadata] = None

    def __repr__(self):
        return f"<EncapsulatedResponse {self.type.name} #{self.seq}>"


class EncapsulatedRequest(EncapsulatedMessage):
    rpc: str

    type: RequestType = RequestType.MESSAGE

    # Optional metadata is available in request.metadata. For streaming requests
    # it can only be sent once.
    metadata: Optional[Metadata] = None

    def __repr__(self):
        return f"<EncapsulatedRequest {self.type.name} #{self.seq}: {self.rpc}>"
