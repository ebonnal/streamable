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

from streamable._stream import stream
from streamable._tools._async import awaitable_to_coroutine
from streamable._tools._iter import SyncAsyncIterable
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


def to_list(stream_: stream[T], itype: IterableType) -> List[T]:
    """Convert a stream to a list, handling both sync and async iterables."""
    assert isinstance(stream_, stream)
    if itype is AsyncIterable:
        return aiterable_to_list(stream_)
    return list(stream_)


async def alist(iterable: AsyncIterable[T]) -> List[T]:
    """Convert an async iterable to a list."""
    return [_ async for _ in iterable]


def bi_iterable_to_iter(
    iterable: Union[SyncAsyncIterable[T], stream[T]], itype: IterableType
) -> Union[Iterator[T], AsyncIterator[T]]:
    """Get an iterator from a bi-iterable (supports both sync and async)."""
    if itype is AsyncIterable:
        return iterable.__aiter__()
    return iter(iterable)


def anext_or_next(it: Union[Iterator[T], AsyncIterator[T]], itype: IterableType) -> T:
    """Get next item from either a sync or async iterator."""
    if itype is AsyncIterable:
        return asyncio.run(
            awaitable_to_coroutine(cast(AsyncIterator[T], it).__anext__())
        )
    return next(cast(Iterator[T], it))


def alist_or_list(iterable: Union[Iterable[T], AsyncIterable[T]]) -> List[T]:
    """Convert an iterable (sync or async) to a list."""
    if isinstance(iterable, AsyncIterable):
        return aiterable_to_list(iterable)
    return list(iterable)
