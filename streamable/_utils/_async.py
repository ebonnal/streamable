import asyncio
from typing import AsyncIterator, Awaitable, TypeVar

T = TypeVar("T")


async def anext(aiterator: AsyncIterator[T]) -> T:
    return await aiterator.__anext__()


async def awaitable_to_coroutine(aw: Awaitable[T]) -> T:
    return await aw


async def empty_aiter() -> AsyncIterator:
    return
    yield  # pragma: no cover


class CloseEventLoopMixin:
    loop: asyncio.AbstractEventLoop  # pragma: no cover

    def __del__(self) -> None:
        if not self.loop.is_closed():
            self.loop.close()
