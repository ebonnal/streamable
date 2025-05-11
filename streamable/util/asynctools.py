import asyncio
from typing import Any, AsyncIterator, Awaitable, Coroutine, Optional, TypeVar

from streamable.util.errors import WrappedError

T = TypeVar("T")

_EVENT_LOOP: Optional[asyncio.AbstractEventLoop] = None


def get_event_loop() -> asyncio.AbstractEventLoop:
    global _EVENT_LOOP
    if not _EVENT_LOOP:
        try:
            _EVENT_LOOP = asyncio.get_running_loop()
        except RuntimeError:
            _EVENT_LOOP = asyncio.new_event_loop()
            asyncio.set_event_loop(_EVENT_LOOP)
    return _EVENT_LOOP


async def awaitable_to_coroutine(aw: Awaitable[T]) -> T:
    return await aw


async def awaitable_to_coroutine_wrapping_stopiteration(aw: Awaitable[T]) -> T:
    try:
        return await aw
    except StopIteration as e:
        raise WrappedError(e) from e


async def empty_aiter() -> AsyncIterator:
    return
    yield
