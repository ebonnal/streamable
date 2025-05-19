import asyncio
from typing import AsyncIterator, Awaitable, Optional, TypeVar

T = TypeVar("T")


async def awaitable_to_coroutine(aw: Awaitable[T]) -> T:
    return await aw


async def empty_aiter() -> AsyncIterator:
    return
    yield


_event_loop: Optional[asyncio.AbstractEventLoop] = None


def get_event_loop() -> asyncio.AbstractEventLoop:
    global _event_loop
    if not _event_loop:
        _event_loop = asyncio.new_event_loop()
    return _event_loop
