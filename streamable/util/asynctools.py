from typing import AsyncIterator, Awaitable, TypeVar

T = TypeVar("T")


async def awaitable_to_coroutine(aw: Awaitable[T]) -> T:
    return await aw


async def empty_aiter() -> AsyncIterator:
    return
    yield
