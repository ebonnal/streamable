import asyncio
from typing import AsyncIterator, Awaitable, Optional, TypeVar

T = TypeVar("T")


async def awaitable_to_coroutine(aw: Awaitable[T]) -> T:
    return await aw


async def empty_aiter() -> AsyncIterator:
    return
    yield


class GetEventLoopMixin:
    _EVENT_LOOP_SINGLETON: Optional[asyncio.AbstractEventLoop] = None

    @classmethod
    def get_event_loop(cls) -> asyncio.AbstractEventLoop:
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            if not cls._EVENT_LOOP_SINGLETON:
                cls._EVENT_LOOP_SINGLETON = asyncio.new_event_loop()
            return cls._EVENT_LOOP_SINGLETON
