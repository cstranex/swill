import sys
import os
import pkgutil
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

    def __init__(self, name: str = "StreamingQueue"):
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
            self._queue.get(), name=f"swill-queue-{self.name}"
        )
        _close_event = asyncio.create_task(
            self._close_event.wait(), name=f"swill-queue-{self.name}"
        )
        _cancel_event = asyncio.create_task(
            self._cancel_event.wait(), name=f"swill-queue-{self.name}"
        )
        self._tasks = [_get_item, _close_event, _cancel_event]

        done, pending = await asyncio.wait(
            self._tasks, return_when=asyncio.FIRST_COMPLETED
        )

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


# Borrowed from flask's implementation
def get_root_path(import_name: str) -> str:
    """Find the root path of a package, or the path that contains a
    module. If it cannot be found, returns the current working
    directory.
    Not to be confused with the value returned by :func:`find_package`.
    :meta private:
    """
    # Module already imported and has a file attribute. Use that first.
    mod = sys.modules.get(import_name)

    if mod is not None and hasattr(mod, "__file__") and mod.__file__ is not None:
        return os.path.dirname(os.path.abspath(mod.__file__))

    # Next attempt: check the loader.
    loader = pkgutil.get_loader(import_name)

    # Loader does not exist or we're referring to an unloaded main
    # module or a main module without path (interactive sessions), go
    # with the current working directory.
    if loader is None or import_name == "__main__":
        return os.getcwd()

    if hasattr(loader, "get_filename"):
        filepath = loader.get_filename(import_name)  # type: ignore
    else:
        # Fall back to imports.
        __import__(import_name)
        mod = sys.modules[import_name]
        filepath = getattr(mod, "__file__", None)

        # If we don't have a file path it might be because it is a
        # namespace package. In this case pick the root path from the
        # first module that is contained in the package.
        if filepath is None:
            raise RuntimeError(
                "No root path can be found for the provided module"
                f" {import_name!r}. This can happen because the module"
                " came from an import hook that does not provide file"
                " name information or because it's a namespace package."
                " In this case the root path needs to be explicitly"
                " provided."
            )

    # filepath is import_name.py for a module, or __init__.py for a package.
    return os.path.dirname(os.path.abspath(filepath))
