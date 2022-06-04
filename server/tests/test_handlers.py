import pytest
import typing as t

from msgspec import Struct

from swill import Request, StreamingRequest, StreamingResponse
from swill._handlers import create_handler
from swill._request import _SwillRequestHandler
from swill._response import Response


def test_handler_is_async():

    def single_request_handler(request: Request):
        return 'test'

    with pytest.raises(ValueError):
        handler = create_handler(single_request_handler)


def test_handler_async_generator():

    async def streaming_response_handler(request: Request) -> StreamingResponse[int]:
        yield 10

    handler = create_handler(streaming_response_handler)


def test_create_single_handler():

    async def single_request_handler(request: Request):
        return 'test'

    handler = create_handler(single_request_handler)

    assert handler == _SwillRequestHandler(
        func=single_request_handler,
        request_type=Request,
        request_message_type=None,
        response_message_type=None,
        request_streams=False,
        response_streams=False,
        uses_response=False
    )


def test_create_single_handler_with_types():

    async def single_request_handler(request: Request[int]) -> str:
        return 'test'

    handler = create_handler(single_request_handler)

    assert handler == _SwillRequestHandler(
        func=single_request_handler,
        request_type=Request,
        request_message_type=int,
        response_message_type=str,
        request_streams=False,
        response_streams=False,
        uses_response=False
    )


def test_create_single_handler_with_complex_types():

    class TestStruct(Struct):
        name: str

    async def single_request_handler(request: Request[t.Dict[str, str]]) -> TestStruct:
        return 'test'

    handler = create_handler(single_request_handler)

    assert handler == _SwillRequestHandler(
        func=single_request_handler,
        request_type=Request,
        request_message_type=t.Dict[str, str],
        response_message_type=TestStruct,
        request_streams=False,
        response_streams=False,
        uses_response=False
    )


def test_create_request_with_response():

    class TestStruct(Struct):
        name: str

    async def single_request_handler(
            request: Request[t.Dict[str, str]],
            response: Response
    ) -> TestStruct:
        return 'test'

    handler = create_handler(single_request_handler)

    assert handler == _SwillRequestHandler(
        func=single_request_handler,
        request_type=Request,
        request_message_type=t.Dict[str, str],
        response_message_type=TestStruct,
        request_streams=False,
        response_streams=False,
        uses_response=True
    )

    async def single_request_handler_different_args(
            request: Request[t.Dict[str, str]],
            response: int
    ) -> TestStruct:
        return 'test'

    handler = create_handler(single_request_handler_different_args)

    assert handler == _SwillRequestHandler(
        func=single_request_handler_different_args,
        request_type=Request,
        request_message_type=t.Dict[str, str],
        response_message_type=TestStruct,
        request_streams=False,
        response_streams=False,
        uses_response=False
    )


def test_create_handler_streaming_request():

    async def single_request_handler(request: StreamingRequest[int]) -> str:
        return 'test'

    handler = create_handler(single_request_handler)

    assert handler == _SwillRequestHandler(
        func=single_request_handler,
        request_type=StreamingRequest,
        request_message_type=int,
        response_message_type=str,
        request_streams=True,
        response_streams=False,
        uses_response=False
    )


def test_create_handler_streaming_response():

    async def single_request_handler(request: Request[int]) -> StreamingResponse[str]:
        return 'test'

    handler = create_handler(single_request_handler)

    assert handler == _SwillRequestHandler(
        func=single_request_handler,
        request_type=Request,
        request_message_type=int,
        response_message_type=str,
        request_streams=False,
        response_streams=True,
        uses_response=False
    )
