import typing as t

import msgspec

from ._exceptions import SwillSerializationError, SwillDeserializationError
from ._types import ErrorMessage
from ._protocol import (
    EncapsulatedResponse,
    EncapsulatedRequest,
    ResponseType,
    EncapsulatedMessage,
)
from .validators import validate_constraints

_encoder = msgspec.msgpack.Encoder()
_decoder = msgspec.msgpack.Decoder(type=EncapsulatedRequest)
_none = _encoder.encode(None)


def deserialize_encapsulated_request(payload: bytes):
    """Deserialize the encapsulated message"""
    try:
        return _decoder.decode(payload)
    except msgspec.DecodeError as e:
        raise SwillDeserializationError(e)


def deserialize_message(
    encapsulated_message: EncapsulatedMessage, request_type: t.Optional[t.Any] = None
):
    """Deserialize the message"""
    try:
        decoded_message = msgspec.msgpack.decode(
            encapsulated_message.data,
            type=request_type,
        )
        # Optionally, perform any constrain validations on the message
        validate_constraints(decoded_message)
        return decoded_message
    except msgspec.DecodeError as e:
        raise SwillDeserializationError(e)


def serialize_message(message: t.Any, message_type: t.Type = None) -> bytes:
    """Serialize the message."""

    # TODO: We should do some validation against message_type
    # so that we can't for example have message_type: Dict[str, str]
    # and then serialize {"Test": 123}

    if origin := t.get_origin(message_type):
        if isinstance(message, origin):
            return _encoder.encode(message)
    elif not message_type or isinstance(message, message_type):
        return _encoder.encode(message)
    elif message_type and isinstance(message, dict):
        return _encoder.encode(message_type(**message))

    raise SwillSerializationError("Message cannot be converted to %s", message_type)


def serialize_response(**kwargs):
    """Serialize a response."""

    kwargs["data"] = msgspec.Raw(kwargs.get("data", _none))

    return _encoder.encode(EncapsulatedResponse(**kwargs))


def serialize_error_response(
    message: str = "", *, code: int, seq: int, data: t.Any = None
):
    """Serialize a standard error message"""
    data = ErrorMessage(code=code, message=message, data=data)
    return _encoder.encode(
        EncapsulatedResponse(
            type=ResponseType.ERROR,
            seq=seq,
            data=data,
        )
    )
