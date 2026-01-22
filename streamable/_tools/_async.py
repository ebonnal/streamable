from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    TypeVar,
)

T = TypeVar("T")
R = TypeVar("R")

AsyncFunction = Callable[[T], Coroutine[Any, Any, R]]


# pre 3.10 to builtin `anext`
async def anext(aiterator: AsyncIterator[T]) -> T:  # pragma: nocover
    return await aiterator.__anext__()


async def awaitable_to_coroutine(aw: Awaitable[T]) -> T:
    return await aw


async def empty_aiter() -> AsyncIterator[Any]:
    return
    yield  # pragma: no cover
