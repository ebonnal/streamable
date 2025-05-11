import asyncio
from typing import (
    AsyncIterable,
    AsyncIterator,
    Callable,
    Iterable,
    Iterator,
    List,
    Set,
    TypeVar,
)

from streamable.util.asynctools import get_event_loop

T = TypeVar("T")


async def _aiter_to_list(aiterable: AsyncIterable[T]) -> List[T]:
    return [elem async for elem in aiterable]


def aiterable_to_list(aiterable: AsyncIterable[T]) -> List[T]:
    return get_event_loop().run_until_complete(_aiter_to_list(aiterable))


async def _aiter_to_set(aiterable: AsyncIterable[T]) -> Set[T]:
    return {elem async for elem in aiterable}


def aiterable_to_set(aiterable: AsyncIterable[T]) -> Set[T]:
    return get_event_loop().run_until_complete(_aiter_to_set(aiterable))


class BiIterable(Iterable[T], AsyncIterable[T]):
    pass


class BiIterator(Iterator[T], AsyncIterator[T]):
    pass


class SyncToBiIterable(BiIterable[T]):
    def __init__(self, iterable: Iterable[T]):
        self.iterable = iterable

    def __iter__(self) -> Iterator[T]:
        return iter(self.iterable)

    def __aiter__(self) -> AsyncIterator[T]:
        return SyncToAsyncIterator(self.iterable)


sync_to_bi_iterable: Callable[[Iterable[T]], BiIterable[T]] = SyncToBiIterable


class SyncToAsyncIterator(AsyncIterator[T]):
    def __init__(self, iterator: Iterable[T]):
        self.iterator: Iterator[T] = iter(iterator)

    async def __anext__(self) -> T:
        try:
            return next(self.iterator)
        except StopIteration as e:
            raise StopAsyncIteration() from e


sync_to_async_iter: Callable[[Iterable[T]], AsyncIterator[T]] = SyncToAsyncIterator


class AsyncToSyncIterator(Iterator[T]):
    def __init__(self, iterator: AsyncIterable[T]):
        self.iterator: AsyncIterator[T] = iterator.__aiter__()
        self.event_loop: asyncio.AbstractEventLoop = get_event_loop()

    def __next__(self) -> T:
        try:
            return self.event_loop.run_until_complete(self.iterator.__anext__())
        except StopAsyncIteration as e:
            raise StopIteration() from e


async_to_sync_iter: Callable[[AsyncIterable[T]], Iterator[T]] = AsyncToSyncIterator
