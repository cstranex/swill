"""Serialization Tests"""
from typing import Union, Dict

import msgspec
import pytest

from swill import _serialize
from swill._exceptions import SwillSerializationError, SwillDeserializationError
from swill._protocol import ResponseType
from swill._request import RequestType


class ExampleStruct(msgspec.Struct):
    name: str
    value: int


class DifferentExampleStruct(msgspec.Struct):
    number: int


def test_deserialize_encapsulated_request():
    payload = b"\x93\x01\x92\x02\x04\xa3add"
    message = _serialize.deserialize_encapsulated_request(payload)
    assert isinstance(message, _serialize.EncapsulatedRequest)
    assert message.rpc == "add"
    assert message.seq == 1
    assert RequestType.MESSAGE == message.type
    assert isinstance(message.data, msgspec.Raw)
    assert message.metadata is None


def test_deserialize_encapsulated_request_with_metadata():
    payload = b"\x95\x05\x05\xa5lines\x00\x81\xa6return\xa8advanced"
    message = _serialize.deserialize_encapsulated_request(payload)
    assert message.metadata == {"return": "advanced"}


def test_serialize_deserialize_encapsulation():
    """Test encoding and decoding an encapsulated message"""
    result = _serialize.deserialize_encapsulated_request(
        _serialize.serialize_message(
            _serialize.EncapsulatedRequest(seq=2, rpc="test", data="DATA"),
            _serialize.EncapsulatedRequest,
        )
    )
    assert result.seq == 2
    assert result.rpc == "test"


def test_deserialize_simple_message():
    request = _serialize.EncapsulatedRequest(seq=1, rpc="test", data=b"\x02")
    message = _serialize.deserialize_message(request, int)
    assert isinstance(message, int)
    assert message == 2


def test_deserialize_struct():
    # Deserialize a Struct
    request = _serialize.EncapsulatedRequest(
        seq=1, rpc="test", data=b"\x82\xa4name\xa4test\xa5value\x05"
    )
    message = _serialize.deserialize_message(request, ExampleStruct)
    assert isinstance(message, ExampleStruct)
    assert message.name == "test"

    # Deserialize a Union
    request = _serialize.EncapsulatedRequest(seq=1, rpc="test", data=b"\x05")
    message = _serialize.deserialize_message(request, Union[int, str])
    assert message == 5


def test_deserialize_invalid():
    request = _serialize.EncapsulatedRequest(seq=1, rpc="test", data=b"\x05")
    with pytest.raises(SwillDeserializationError):
        _serialize.deserialize_message(request, ExampleStruct)


def test_deserialize_missing():
    # No data but expecting a type
    request = _serialize.EncapsulatedRequest(seq=1, rpc="test", data=b"")
    with pytest.raises(SwillDeserializationError):
        _serialize.deserialize_message(request, ExampleStruct)

    # No data and not expecting
    with pytest.raises(SwillDeserializationError):
        _serialize.deserialize_message(request, None)

    request = _serialize.EncapsulatedRequest(seq=1, rpc="test", data=b"\xc0")
    _serialize.deserialize_message(request, None)

    # Data but not expecting
    request = _serialize.EncapsulatedRequest(seq=1, rpc="test", data=b"\x05")
    with pytest.raises(SwillDeserializationError):
        _serialize.deserialize_message(request, None)


def test_serialize_simple():
    assert _serialize.serialize_message("test", str) == b"\xa4test"


def test_serialize_struct():
    """Test direct serialization of struct"""
    struct = ExampleStruct(name="test", value=5)
    assert (
        _serialize.serialize_message(struct, ExampleStruct)
        == b"\x82\xa4name\xa4test\xa5value\x05"
    )


def test_serialize_dict():
    """Test serialization of dictionaries"""

    assert (
        _serialize.serialize_message({"name": "test", "value": 5}, ExampleStruct)
        == b"\x82\xa4name\xa4test\xa5value\x05"
    )

    # Test dict serialization using generic dict type
    assert (
        _serialize.serialize_message(
            {"name": "test", "value": 5}, Dict[str, Union[str, int]]
        )
        == b"\x82\xa4name\xa4test\xa5value\x05"
    )


def test_serialize_none():
    """Test serialization of None"""
    assert _serialize.serialize_message(None) == b"\xc0"


def test_serialize_none_with_struct():
    with pytest.raises(SwillSerializationError):
        _serialize.serialize_message(None, ExampleStruct)


def test_serialize_invalid_type():
    with pytest.raises(SwillSerializationError):
        _serialize.serialize_message(None, Dict[str, str])

    with pytest.raises(SwillSerializationError):
        _serialize.serialize_message("test", int)


@pytest.mark.xfail(reason="Generic types aren't validated yet")
def test_serialize_invalid_complex_type():
    with pytest.raises(SwillSerializationError):
        _serialize.serialize_message({"name": 123}, Dict[str, str])


def test_serialize_response_with_data():
    serialized = _serialize.serialize_response(
        seq=1, data=b"\x82\xa4name\xa4test\xa5value\x05"
    )

    message = msgspec.msgpack.decode(serialized)
    assert message == [1, {"name": "test", "value": 5}, 0, None, None]

    _decoder = msgspec.msgpack.Decoder(type=_serialize.EncapsulatedResponse)
    data = _decoder.decode(serialized)
    assert data.seq == 1
    assert data.type == ResponseType.MESSAGE


def test_serialize_response_without_data():
    serialized = _serialize.serialize_response(
        seq=1,
    )

    message = msgspec.msgpack.decode(serialized)

    assert [1, None, 0, None, None] == message


def test_serialize_error_response():
    serialized = _serialize.serialize_error_response("test", code=400, seq=1)
    message = msgspec.msgpack.decode(serialized)
    assert message == [
        1,
        {
            "message": "test",
            "code": 400,
        },
        3,
        None,
        None,
    ]
    _decoder = msgspec.msgpack.Decoder(type=_serialize.EncapsulatedResponse)
    data = _decoder.decode(serialized)
    assert data.type == ResponseType.ERROR
