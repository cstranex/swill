import typing as t
from contextlib import asynccontextmanager

from swill._exceptions import SwillRequestCancelled


@asynccontextmanager
async def closing_response(generator_or_coro: t.Union[t.AsyncGenerator, t.Awaitable]):

    if not isinstance(generator_or_coro, t.AsyncGenerator):
        return

    try:
        yield generator_or_coro
    except (SwillRequestCancelled, StopAsyncIteration):
        pass
    finally:
        await generator_or_coro.aclose()
