import asyncio
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


class StreamingQueue(t.AsyncIterable):
    """A queue of incoming messages for a StreamingRequest to process"""

    def __init__(self, name: str = 'StreamingQueue'):
        self.name = name
        self._queue = asyncio.Queue()
        self._close_event = asyncio.Event()
        self._cancel_event = asyncio.Event()
        self._tasks = []

    def add(self, message):
        """Add message to the queue"""
        self._queue.put_nowait(message)

    def close(self):
        """Close the streaming queue.
        This will cause the generator to raise StopAsyncIteration."""
        self._close_event.set()

    def cancel(self):
        """Cancel the streaming queue

        This will cause the generator to raise SwillRequestCancelled"""
        self._cancel_event.set()

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._cancel_event.is_set():
            raise SwillRequestCancelled()

        _get_item = asyncio.create_task(
            self._queue.get(),
            name=f'swill-queue-{self.name}'
        )
        _close_event = asyncio.create_task(
            self._close_event.wait(),
            name=f'swill-queue-{self.name}'
        )
        _cancel_event = asyncio.create_task(
            self._cancel_event.wait(),
            name=f'swill-queue-{self.name}'
        )
        self._tasks = [_get_item, _close_event, _cancel_event]

        done, pending = await asyncio.wait(self._tasks, return_when=asyncio.FIRST_COMPLETED)

        for task in pending:
            task.cancel()
            self._tasks = []

        if _cancel_event in done:
            raise SwillRequestCancelled()

        if _get_item in done:
            return _get_item.result()

        if _close_event in done:
            raise StopAsyncIteration()

        raise StopAsyncIteration()
