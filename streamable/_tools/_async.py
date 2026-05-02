from asyncio import Semaphore
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Literal,
    TypeVar,
)

T = TypeVar("T")
R = TypeVar("R")

AsyncFunction = Callable[[T], Coroutine[object, object, R]]


# `builtins.anext` for pre 3.10
async def anext(aiterator: AsyncIterator[T]) -> T:  # pragma: nocover
    return await aiterator.__anext__()


async def awaitable_to_coroutine(aw: Awaitable[T]) -> T:
    return await aw


async def empty_aiter() -> AsyncIterator[Any]:
    return
    yield  # pragma: no cover


class NoopSemaphore(Semaphore):
    __slots__ = ()

    def __init__(self) -> None:
        pass

    async def acquire(self) -> Literal[True]:
        return True

    def release(self) -> None:
        return

    def locked(self) -> bool:
        return False
