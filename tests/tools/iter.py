"""Helpers for converting between sync and async iterables."""

from typing import (
    AsyncIterable,
    AsyncIterator,
    Callable,
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

from streamable._tools._iter import SyncAsyncIterable, SyncToAsyncIterator
from tests.tools.loop import TEST_LOOP

IterableType = Union[Type[Iterable], Type[AsyncIterable]]
ITERABLE_TYPES: Tuple[IterableType, ...] = (Iterable, AsyncIterable)

T = TypeVar("T")


async def _aiter_to_list(aiterable: AsyncIterable[T]) -> List[T]:
    return [elem async for elem in aiterable]


def aiterable_to_list(aiterable: AsyncIterable[T]) -> List[T]:
    return TEST_LOOP.run_until_complete(_aiter_to_list(aiterable.__aiter__()))


def stopiteration_type(itype: IterableType) -> Type[Exception]:
    """Return the appropriate StopIteration type for the iterable type."""
    if issubclass(itype, AsyncIterable):
        return StopAsyncIteration
    return StopIteration


async def alist(iterable: AsyncIterable[T]) -> List[T]:
    """Convert an async iterable to a list."""
    return [_ async for _ in iterable]


async def acount(iterator: AsyncIterable) -> int:
    count = 0
    async for _ in iterator:
        count += 1
    return count


def aiter_or_iter(
    iterable: Union[Iterable[T], AsyncIterable[T]], itype: IterableType
) -> Union[Iterator[T], AsyncIterator[T]]:
    """Get an iterator from an iterable, sync or async according to `itype`."""
    if itype is AsyncIterable:
        return cast(AsyncIterator[T], iterable).__aiter__()
    return cast(Iterator[T], iterable).__iter__()


def anext_or_next(it: Union[Iterator[T], AsyncIterator[T]], itype: IterableType) -> T:
    """Get next item from either a sync or async iterator."""
    if itype is AsyncIterable:
        return TEST_LOOP.run_until_complete(
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
        TEST_LOOP.run_until_complete(awaitable_to_coroutine(s))
    s()


class SyncToBiIterable(SyncAsyncIterable[T]):
    """Wrapper to make a sync iterable also async-iterable."""

    def __init__(self, iterable: Iterable[T]):
        self.iterable = iterable

    def __iter__(self) -> Iterator[T]:
        return self.iterable.__iter__()

    def __aiter__(self) -> AsyncIterator[T]:
        return SyncToAsyncIterator(self.iterable.__iter__())


sync_to_bi_iterable: Callable[[Iterable[T]], SyncAsyncIterable[T]] = SyncToBiIterable
