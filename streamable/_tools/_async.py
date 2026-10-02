from typing import Any, AsyncIterator, Awaitable, Callable, Coroutine, TypeVar

from streamable._tools._sentinel import NOT_SPECIFIED

T = TypeVar("T")
R = TypeVar("R")

AsyncFunction = Callable[[T], Coroutine[object, object, R]]


# `builtins.anext` for pre 3.10
async def anext(aiterator: AsyncIterator[T], default: T = NOT_SPECIFIED) -> T:
    try:
        return await aiterator.__anext__()
    except StopAsyncIteration:
        if default is NOT_SPECIFIED:
            raise
        return default


async def awaitable_to_coroutine(aw: Awaitable[T]) -> T:
    return await aw


async def empty_aiter() -> AsyncIterator[Any]:
    return
    yield  # pragma: no cover
