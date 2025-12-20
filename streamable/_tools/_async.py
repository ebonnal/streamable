import asyncio
from typing import Any, AsyncIterator, Awaitable, TypeVar

T = TypeVar("T")


# pre 3.10 to builtin `anext`
async def anext(aiterator: AsyncIterator[T]) -> T:  # pragma: nocover
    return await aiterator.__anext__()


async def awaitable_to_coroutine(aw: Awaitable[T]) -> T:
    return await aw


async def empty_aiter() -> AsyncIterator[Any]:
    return
    yield  # pragma: no cover


class CloseEventLoopMixin:
    # Instance variable that subclasses must set in __init__
    loop: asyncio.AbstractEventLoop  # pragma: no cover

    def __del__(self) -> None:
        if not self.loop.is_closed():
            self.loop.close()
