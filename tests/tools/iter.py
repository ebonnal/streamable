import asyncio
import time
from typing import (
    AsyncIterable,
    AsyncIterator,
    Callable,
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

from streamable._stream import stream
from streamable._tools._async import AsyncFunction
from streamable._tools._iter import (
    AsyncClosable,
    Closable,
    ClosableAsyncIterator,
    ClosableIterator,
)
from tests.tools.func import async_identity, identity
from tests.tools.loop import TEST_LOOP

IterableType = Literal["sync", "async"]
ITERABLE_TYPES: Tuple[IterableType, ...] = ("sync", "async")

T = TypeVar("T")


def aidentity_or_identity(
    itype: IterableType,
) -> Union[Callable[[T], T], AsyncFunction[T, T]]:
    assert itype in ITERABLE_TYPES
    if itype == "async":
        return async_identity
    return identity


async def _aiter_to_list(aiterable: AsyncIterable[T]) -> List[T]:
    return [elem async for elem in aiterable]


def aiterable_to_list(aiterable: AsyncIterable[T]) -> List[T]:
    return TEST_LOOP.run_until_complete(_aiter_to_list(aiterable.__aiter__()))


def stopiteration_type(itype: IterableType) -> Type[Exception]:
    assert itype in ITERABLE_TYPES
    if itype == "async":
        return StopAsyncIteration
    return StopIteration


async def acount(iterator: AsyncIterable) -> int:
    count = 0
    async for _ in iterator:
        count += 1
    return count


def aiter_or_iter(
    s: stream[T], itype: IterableType
) -> Union[ClosableIterator[T], ClosableAsyncIterator[T]]:
    assert itype in ITERABLE_TYPES
    if itype == "async":
        return s.__aiter__()
    return s.__iter__()


def anext_or_next(it: Union[Iterator[T], AsyncIterator[T]], itype: IterableType) -> T:
    assert itype in ITERABLE_TYPES
    if itype == "async":
        return TEST_LOOP.run_until_complete(cast(AsyncIterator[T], it).__anext__())
    return next(cast(Iterator[T], it))


def aclose_or_close(obj: Union[Closable, AsyncClosable], itype: IterableType) -> None:
    assert itype in ITERABLE_TYPES
    if itype == "async":
        TEST_LOOP.run_until_complete(cast(AsyncClosable, obj).aclose())
    else:
        cast(Closable, obj).close()


def asleep_or_sleep(seconds: float, itype: IterableType) -> None:
    assert itype in ITERABLE_TYPES
    if itype == "async":
        TEST_LOOP.run_until_complete(asyncio.sleep(seconds))
    else:
        time.sleep(seconds)


def alist_or_list(
    iterable: Union[Iterable[T], AsyncIterable[T]], itype: IterableType
) -> List[T]:
    assert itype in ITERABLE_TYPES
    if itype == "async":
        return aiterable_to_list(cast(AsyncIterable[T], iterable))
    return list(cast(Iterable[T], iterable))


class SyncToAsyncIterator(AsyncIterator[T]):
    __slots__ = ("upstream",)

    def __init__(self, upstream: Iterator[T]):
        self.upstream = upstream

    async def __anext__(self) -> T:
        try:
            return self.upstream.__next__()
        except StopIteration as e:
            raise StopAsyncIteration from e


class SyncToAsyncIterable(AsyncIterable[T]):
    def __init__(self, iterable: Iterable[T]) -> None:
        self._iterable = iterable

    def __aiter__(self) -> AsyncIterator[T]:
        return SyncToAsyncIterator(self._iterable.__iter__())


def aiterable_or_iterable(
    iterable: Iterable[T], itype: IterableType
) -> Union[Iterable[T], AsyncIterable[T]]:
    assert itype in ITERABLE_TYPES
    if itype == "async":
        return SyncToAsyncIterable(iterable)
    return iterable


def to_aiter(iterable: Union[Iterable[T], AsyncIterable[T]]) -> AsyncIterator[T]:
    if isinstance(iterable, AsyncIterable):
        return iterable.__aiter__()
    return SyncToAsyncIterator(iterable.__iter__())
