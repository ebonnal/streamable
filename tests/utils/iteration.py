"""Helpers for converting between sync and async iterables."""

import asyncio
from typing import (
    AsyncIterable,
    AsyncIterator,
    Iterator,
    Iterable,
    List,
    Type,
    TypeVar,
    Union,
    cast,
)

from streamable import stream
from streamable._tools._async import awaitable_to_coroutine
from typing import Tuple

IterableType = Union[Type[Iterable], Type[AsyncIterable]]
ITERABLE_TYPES: Tuple[IterableType, ...] = (Iterable, AsyncIterable)

T = TypeVar("T")


async def _aiter_to_list(aiterable: AsyncIterable[T]) -> List[T]:
    return [elem async for elem in aiterable]


def aiterable_to_list(aiterable: AsyncIterable[T]) -> List[T]:
    return asyncio.run(_aiter_to_list(aiterable))


def stopiteration_type(itype: IterableType) -> Type[Exception]:
    """Return the appropriate StopIteration type for the iterable type."""
    if issubclass(itype, AsyncIterable):
        return StopAsyncIteration
    return StopIteration


async def alist(iterable: AsyncIterable[T]) -> List[T]:
    """Convert an async iterable to a list."""
    return [_ async for _ in iterable]


def aiter_or_iter(
    iterable: Union[Iterable[T], AsyncIterable[T]], itype: IterableType
) -> Union[Iterator[T], AsyncIterator[T]]:
    """Get an iterator from a bi-iterable (supports both sync and async)."""
    if itype is AsyncIterable:
        return cast(AsyncIterator[T], iterable).__aiter__()
    return cast(Iterator[T], iterable).__iter__()


def anext_or_next(it: Union[Iterator[T], AsyncIterator[T]], itype: IterableType) -> T:
    """Get next item from either a sync or async iterator."""
    if itype is AsyncIterable:
        return asyncio.run(
            awaitable_to_coroutine(cast(AsyncIterator[T], it).__anext__())
        )
    return next(cast(Iterator[T], it))


def alist_or_list(
    iterable: Union[Iterable[T], AsyncIterable[T]], itype: IterableType
) -> List[T]:
    """Convert an iterable (sync or async) to a list."""
    if itype is AsyncIterable:
        return aiterable_to_list(cast(AsyncIterable[T], iterable))
    return list(cast(Iterable[T], iterable))


def aiterate_or_iterate(s: stream, itype: IterableType) -> None:
    """Convert an iterable (sync or async) to a list."""
    if itype is AsyncIterable:
        asyncio.run(awaitable_to_coroutine(s))
    s()
