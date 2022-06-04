import pytest
import typing as t
from swill import Swill, Request
from swill.testing import SwillTestClient


async def add(request: Request[t.List[int]]) -> int:
    return sum(request.data)


@pytest.mark.asyncio
async def test_simple_request():
    swill = Swill(__name__)
    swill.add_handler(add)

    async with SwillTestClient(swill) as client:
        request = client.request('add', [1, 2])
        with request:
            assert (await request.receive())['data'] == 3
