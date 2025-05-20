import asyncio
from typing import AsyncIterator, Awaitable, Optional, Set, TypeVar

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
        if self._event_loop:
            pending_tasks: Set[asyncio.Task] = asyncio.all_tasks(self._event_loop)
            if pending_tasks:
                for task in pending_tasks:
                    task.cancel()
                self._event_loop.run_until_complete(asyncio.gather(*pending_tasks))

            self._event_loop.close()
