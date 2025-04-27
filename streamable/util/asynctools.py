import asyncio
from typing import Awaitable, TypeVar

T = TypeVar("T")

async def awaitable_to_coroutine(aw: Awaitable[T]) -> T:
    return await aw

def await_result(aw: Awaitable[T]) -> T:
    return asyncio.run(awaitable_to_coroutine(aw))