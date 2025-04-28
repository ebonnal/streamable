import asyncio
from typing import AsyncIterator, Awaitable, Optional, TypeVar

T = TypeVar("T")

_EVENT_LOOP: Optional[asyncio.AbstractEventLoop] = None

def get_event_loop() -> asyncio.AbstractEventLoop:
    global _EVENT_LOOP
    if not _EVENT_LOOP:
        try:
            _EVENT_LOOP = asyncio.get_event_loop()
        except RuntimeError:
            _EVENT_LOOP = asyncio.new_event_loop()
            asyncio.set_event_loop(_EVENT_LOOP)
    return _EVENT_LOOP

async def awaitable_to_coroutine(aw: Awaitable[T]) -> T:
    return await aw


def await_result(aw: Awaitable[T]) -> T:
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(awaitable_to_coroutine(aw))
