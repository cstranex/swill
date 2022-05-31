import typing
from typing import Any, Optional, Type

import msgspec

from ._exceptions import SwillSerializationError, SwillDeserializationError
from ._types import ErrorMessage
from ._protocol import EncapsulatedResponse, EncapsulatedRequest, ResponseType

_encoder = msgspec.msgpack.Encoder()
_decoder = msgspec.msgpack.Decoder(type=EncapsulatedRequest)
_none = _encoder.encode(None)


def deserialize_encapsulated_request(payload: bytes):
    """Deserialize the encapsulated message"""
    try:
        return _decoder.decode(payload)
    except msgspec.DecodeError as e:
        raise SwillDeserializationError(e)


def deserialize_message(encapsulated_message: EncapsulatedRequest, request_type: Optional[Any] = None):
    """Deserialize the message """
    try:
        return msgspec.msgpack.decode(encapsulated_message.data, type=request_type)
    except msgspec.DecodeError as e:
        raise SwillDeserializationError(e)


def serialize_message(message: Any, message_type: Type = None) -> bytes:
    """Serialize the message."""

    # TODO: We should do some validation against message_type
    # so that we can't for example have message_type: Dict[str, str]
    # and then serialize {"Test": 123}

    if origin := typing.get_origin(message_type):
        if isinstance(message, origin):
            return _encoder.encode(message)
    elif not message_type or isinstance(message, message_type):
        return _encoder.encode(message)
    elif message_type and isinstance(message, dict):
        return _encoder.encode(message_type(**message))

    raise SwillSerializationError("Message cannot be converted to %s", message_type)


def serialize_response(**kwargs):
    """Serialize a response."""

    kwargs['data'] = msgspec.Raw(kwargs.get('data', _none))

    return _encoder.encode(EncapsulatedResponse(**kwargs))


def serialize_error_response(message: str = '', *, code: int, seq: int, data: Any = None):
    """Serialize a standard error message"""
    data = ErrorMessage(code=code, message=message, data=data)
    return _encoder.encode(EncapsulatedResponse(
        type=ResponseType.ERROR,
        seq=seq,
        data=data,
    ))
