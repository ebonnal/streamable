from typing import AsyncIterator, Callable, Coroutine, TypeVar

T = TypeVar("T")
R = TypeVar("R")

AsyncFunction = Callable[[T], Coroutine[object, object, R]]


# `builtins.anext` for pre 3.10
async def anext(aiterator: AsyncIterator[T], default: T) -> T:
    try:
        return await aiterator.__anext__()
    except StopAsyncIteration:
        return default
