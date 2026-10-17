from typing import (
    AsyncIterable,
    AsyncIterator,
    Iterable,
    Iterator,
    List,
    Literal,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from streamable._tools._iter import SyncAsyncIterable, SyncToAsyncIterator
from tests.tools.loop import TEST_LOOP

IterableType = Literal["sync", "async"]
ITERABLE_TYPES: Tuple[IterableType, ...] = ("sync", "async")

T = TypeVar("T")


async def _aiter_to_list(aiterable: AsyncIterable[T]) -> List[T]:
    return [elem async for elem in aiterable]


def aiterable_to_list(aiterable: AsyncIterable[T]) -> List[T]:
    return TEST_LOOP.run_until_complete(_aiter_to_list(aiterable.__aiter__()))


def stopiteration_type(itype: IterableType) -> Type[Exception]:
    assert itype in ITERABLE_TYPES
    if itype == "async":
        return StopAsyncIteration
    return StopIteration


async def alist(iterable: AsyncIterable[T]) -> List[T]:
    return [_ async for _ in iterable]


async def acount(iterator: AsyncIterable) -> int:
    count = 0
    async for _ in iterator:
        count += 1
    return count


def aiter_or_iter(
    iterable: Union[Iterable[T], AsyncIterable[T]], itype: IterableType
) -> Union[Iterator[T], AsyncIterator[T]]:
    assert itype in ITERABLE_TYPES
    if itype == "async":
        return cast(AsyncIterator[T], iterable).__aiter__()
    return cast(Iterator[T], iterable).__iter__()


def anext_or_next(it: Union[Iterator[T], AsyncIterator[T]], itype: IterableType) -> T:
    assert itype in ITERABLE_TYPES
    if itype == "async":
        return TEST_LOOP.run_until_complete(cast(AsyncIterator[T], it).__anext__())
    return next(cast(Iterator[T], it))


def alist_or_list(
    iterable: Union[Iterable[T], AsyncIterable[T]], itype: IterableType
) -> List[T]:
    assert itype in ITERABLE_TYPES
    if itype == "async":
        return aiterable_to_list(cast(AsyncIterable[T], iterable))
    return list(cast(Iterable[T], iterable))


class SyncToBiIterable(SyncAsyncIterable[T]):
    def __init__(self, iterable: Iterable[T]):
        self.iterable = iterable

    def __iter__(self) -> Iterator[T]:
        return self.iterable.__iter__()

    def __aiter__(self) -> AsyncIterator[T]:
        return SyncToAsyncIterator(self.iterable.__iter__())


def sync_to_bi_iterable(iterable: Iterable[T]) -> SyncAsyncIterable[T]:
    return SyncToBiIterable(iterable)


def async_iter(iterable: Union[Iterable[T], AsyncIterable[T]]) -> AsyncIterator[T]:
    if isinstance(iterable, AsyncIterable):
        return iterable.__aiter__()
    return SyncToAsyncIterator(iterable.__iter__())
