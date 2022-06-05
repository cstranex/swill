from typing import AsyncIterator

import pytest
import typing as t
from swill import Swill, Request, StreamingRequest
from swill._response import Response
from swill.testing import SwillTestClient
from tests.helpers import with_timeout


async def streaming_request_response(
    request: StreamingRequest[int], response: Response
) -> AsyncIterator[int]:
    async for value in request.data:
        yield value
    response.set_trailing_metadata({"key": "value"})


async def add(request: Request[t.List[int]]) -> int:
    return sum(request.data)


@pytest.mark.asyncio
async def test_simple_request():
    swill = Swill(__name__)
    swill.add_handler(add)

    async with SwillTestClient(swill) as client:
        async with client.request("add", [1, 2]) as request:
            assert (await request.receive())["data"] == 3


@pytest.mark.asyncio
@with_timeout(1)
async def test_all_lifecycle_handlers(mocker):
    swill = Swill(__name__)
    swill.add_handler(streaming_request_response)

    swill._call_lifecycle_handlers = mocker.AsyncMock()

    async with SwillTestClient(swill) as client:
        async with client.request("streaming_request_response", 1) as request:
            await request.send(2)
            await request.receive()
            await request.end_of_stream()

    calls = [
        mocker.call("before_connection", mocker.ANY),
        mocker.call("after_accept", mocker.ANY),
        mocker.call("before_request", mocker.ANY, mocker.ANY, mocker.ANY),
        mocker.call("before_request_message", mocker.ANY, 1),  # (1)
        mocker.call("before_response_message", mocker.ANY, 1),  # yielding 1
        mocker.call("before_leading_metadata", mocker.ANY, None),  # leading metadata
        mocker.call("before_request_data", mocker.ANY, mocker.ANY),  # (2)
        mocker.call("before_request_message", mocker.ANY, 2),  # (2)
        mocker.call("before_request_data", mocker.ANY, mocker.ANY),  # (END_OF_STREAM)
        mocker.call("before_response_message", mocker.ANY, 2),  # yielding 2
        mocker.call(
            "before_trailing_metadata", mocker.ANY, {"key": "value"}
        ),  # yielding 2
        mocker.call("after_request", mocker.ANY),
        mocker.call("after_connection", mocker.ANY),
    ]

    swill._call_lifecycle_handlers.assert_has_awaits(calls)
