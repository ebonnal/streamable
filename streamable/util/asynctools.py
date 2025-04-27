import asyncio
from typing import Awaitable, TypeVar

T = TypeVar("T")

async def awaitable_to_coroutine(aw: Awaitable[T]) -> T:
    return await aw

def await_result(aw: Awaitable[T]) -> T:
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(awaitable_to_coroutine(aw))