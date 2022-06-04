"""Test the SteamingQueue helper async generator"""
import asyncio
import pytest

from swill._exceptions import SwillRequestCancelled
from swill._helpers import StreamingQueue


@pytest.mark.asyncio
async def test_streaming_queue_messages(event_loop):
    """Test the streaming queue consuming data"""
    queue = StreamingQueue()
    queue.add('test')
    queue.add('second')
    queue.add('third')

    item = await queue.__anext__()
    assert item == 'test'

    item = await queue.__anext__()
    assert item == 'second'

    item = await queue.__anext__()
    assert item == 'third'

    fut = asyncio.ensure_future(queue.__anext__(), loop=event_loop)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(fut, timeout=0.1)
    for task in queue._tasks:
        task.cancel()


@pytest.mark.asyncio
async def test_streaming_queue_close():
    """Test the streaming queue consuming data and closing"""
    queue = StreamingQueue()
    queue.add('test')
    queue.add('second')

    item = await queue.__anext__()
    assert item == 'test'

    queue.close()

    with pytest.raises(StopAsyncIteration):
        await queue.__anext__()
    for task in queue._tasks:
        task.cancel()

@pytest.mark.asyncio
async def test_streaming_queue_cancel():
    """Test the streaming queue consuming data and closing"""
    queue = StreamingQueue()
    queue.add('test')
    queue.add('second')

    item = await queue.__anext__()
    assert item == 'test'

    queue.cancel()

    with pytest.raises(SwillRequestCancelled):
        item = await queue.__anext__()
    for task in queue._tasks:
        task.cancel()


@pytest.mark.asyncio
async def test_streaming_queue_close_while_waiting(event_loop):
    queue = StreamingQueue()

    next_future = asyncio.ensure_future(queue.__anext__())

    async def cancel_request():
        await asyncio.sleep(0.2)
        queue.cancel()
        assert next_future.done() == False

    cancel_future = asyncio.ensure_future(cancel_request(), loop=event_loop)

    asyncio.set_event_loop(event_loop)
    done, pending = await asyncio.wait([next_future, cancel_future])
    assert queue._cancel_event.is_set()
    assert len(done) == 2
    assert isinstance(next_future.exception(), SwillRequestCancelled)
    assert len(pending) == 0
