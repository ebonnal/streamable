import asyncio
import queue
import sys
import time
from typing import (
    AsyncIterable,
    AsyncIterator,
    Callable,
    Coroutine,
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

from streamable import stream, stream_aiterator, stream_iterator
from streamable._tools._async import AsyncFunction
from streamable._tools._iter import AsyncClosable, Closable
from tests.tools.error import TestError
from tests.tools.func import (
    async_identity,
    async_slow_identity,
    identity,
    slow_identity,
)
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


def aqueue_or_queue(
    itype: IterableType,
) -> Union["asyncio.Queue[T]", "queue.Queue[T]"]:
    assert itype in ITERABLE_TYPES
    if itype == "async":
        if sys.version_info < (3, 10):
            return asyncio.Queue(loop=TEST_LOOP)
        return asyncio.Queue()
    return queue.Queue()


def aiter_or_iter(
    s: stream[T], itype: IterableType
) -> Union[stream_iterator[T], stream_aiterator[T]]:
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


def aslow_or_slow_identity(
    itype: IterableType,
) -> Union[Callable[[T], Coroutine[object, object, T]], Callable[[T], T]]:
    assert itype in ITERABLE_TYPES
    if itype == "async":
        return async_slow_identity
    else:
        return slow_identity


def alist_or_list(
    iterable: Union[Iterable[T], AsyncIterable[T]], itype: IterableType
) -> List[T]:
    assert itype in ITERABLE_TYPES
    if itype == "async":
        return aiterable_to_list(cast(AsyncIterable[T], iterable))
    return list(cast(Iterable[T], iterable))


def await_or_call(s: "stream", itype: IterableType) -> None:
    if itype == "async":
        TEST_LOOP.run_until_complete(s)
    else:
        s()


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


class RaisingOnIterIterable(Iterable[T]):
    def __iter__(self) -> Iterator[T]:
        raise TestError


class RaisingOnAiterAsyncIterable(AsyncIterable[T]):
    def __aiter__(self) -> AsyncIterator[T]:
        raise TestError
