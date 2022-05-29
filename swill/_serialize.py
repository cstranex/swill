from typing import Any, Optional, Type

import msgspec

from ._types import ErrorMessage
from ._protocol import EncapsulatedResponse, EncapsulatedRequest, ResponseType

_encoder = msgspec.msgpack.Encoder()
_decoder = msgspec.msgpack.Decoder(type=EncapsulatedRequest)


def deserialize_encapsulated_request(payload: bytes):
    """Deserialize the encapsulated message"""
    return _decoder.decode(payload)


def deserialize_message(encapsulated_message: EncapsulatedRequest, request_type: Optional[Any] = None):
    """Deserialize the message """
    return msgspec.msgpack.decode(encapsulated_message.data, type=request_type)


def serialize_message(message: Any, message_type: Type[msgspec.Struct]) -> bytes:
    """Serialize the message"""
    if isinstance(message_type, msgspec.Struct):
        return _encoder.encode(message_type(**message))
    else:
        return _encoder.encode(message)


def serialize_response(**kwargs):
    """Serialize a response."""

    kwargs['data'] = msgspec.Raw(kwargs.get('data', _encoder.encode(None)))

    # data=data, error=error, seq=seq, end_of_stream=end_of_stream))
    return _encoder.encode(EncapsulatedResponse(**kwargs))


def serialize_error_response(message: str = '', *, code: int, seq: int):
    """Serialize a standard error message"""
    data = ErrorMessage(code=code, message=message)
    return _encoder.encode(EncapsulatedResponse(
        type=ResponseType.ERROR,
        seq=seq,
        data=data,
    ))
