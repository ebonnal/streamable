import asyncio
from typing import AsyncIterator, Awaitable, Optional, TypeVar

T = TypeVar("T")


async def awaitable_to_coroutine(aw: Awaitable[T]) -> T:
    return await aw


async def empty_aiter() -> AsyncIterator:
    return
    yield


class EventLoopMixin:
    _event_loop: Optional[asyncio.AbstractEventLoop] = None

    @property
    def event_loop(self) -> asyncio.AbstractEventLoop:
        if not self._event_loop:
            self._event_loop = asyncio.new_event_loop()
        return self._event_loop

    def __del__(self) -> None:
        self.event_loop.close()
