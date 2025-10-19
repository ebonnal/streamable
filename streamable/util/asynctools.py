import asyncio
from typing import AsyncIterator, Awaitable, TypeVar

T = TypeVar("T")


async def awaitable_to_coroutine(aw: Awaitable[T]) -> T:
    return await aw


async def empty_aiter() -> AsyncIterator:
    return
    yield  # pragma: no cover


def close_event_loop(event_loop: asyncio.AbstractEventLoop) -> None:
    if not event_loop.is_closed():
        event_loop.close()


class CloseEventLoopMixin:
    event_loop: asyncio.AbstractEventLoop  # pragma: no cover

    def close_event_loop_on_gc(self) -> None:
        close_event_loop(self.event_loop)
