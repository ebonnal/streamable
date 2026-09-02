from typing import Any, AsyncIterator, Awaitable, Callable, Coroutine, TypeVar

T = TypeVar("T")
R = TypeVar("R")

AsyncFunction = Callable[[T], Coroutine[object, object, R]]


# `builtins.anext` for pre 3.10
async def anext(aiterator: AsyncIterator[T]) -> T:  # pragma: nocover
    return await aiterator.__anext__()


async def aclose(aiterator: AsyncIterator) -> None:
    """Closes `aiterator` if it defines an `aclose` method."""
    method = getattr(aiterator, "aclose", None)
    if method is not None:
        await method()


async def awaitable_to_coroutine(aw: Awaitable[T]) -> T:
    return await aw


async def empty_aiter() -> AsyncIterator[Any]:
    return
    yield  # pragma: no cover
