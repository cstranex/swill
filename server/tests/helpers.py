import asyncio
from functools import wraps


def with_timeout(t):
    def wrapper(corofunc):
        @wraps(corofunc)
        async def run(*args, **kwargs):
            task = asyncio.create_task(corofunc(*args, **kwargs))
            return await asyncio.wait_for(task, timeout=t)

        return run

    return wrapper
