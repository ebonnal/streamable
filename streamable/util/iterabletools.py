import asyncio
from typing import (
    AsyncIterable,
    AsyncIterator,
    Callable,
    Iterable,
    Iterator,
    TypeVar,
)

from streamable.util.asynctools import CloseEventLoopMixin

T = TypeVar("T")


class BiIterable(Iterable[T], AsyncIterable[T]):
    pass


class BiIterator(Iterator[T], AsyncIterator[T]):
    pass


class SyncToBiIterable(BiIterable[T]):
    def __init__(self, iterable: Iterable[T]):
        self.iterable = iterable

    def __iter__(self) -> Iterator[T]:
        return self.iterable.__iter__()

    def __aiter__(self) -> AsyncIterator[T]:
        return SyncToAsyncIterator(self.iterable.__iter__())


sync_to_bi_iterable: Callable[[Iterable[T]], BiIterable[T]] = SyncToBiIterable


class SyncToAsyncIterator(AsyncIterator[T]):
    def __init__(self, iterator: Iterator[T]):
        self.iterator = iterator

    async def __anext__(self) -> T:
        try:
            return self.iterator.__next__()
        except StopIteration as e:
            raise StopAsyncIteration() from e


sync_to_async_iter: Callable[[Iterator[T]], AsyncIterator[T]] = SyncToAsyncIterator


class AsyncToSyncIterator(Iterator[T], CloseEventLoopMixin):
    def __init__(
        self,
        event_loop: asyncio.AbstractEventLoop,
        aiterator: AsyncIterator[T],
    ):
        self.aiterator = aiterator
        self.event_loop = event_loop

    def __next__(self) -> T:
        try:
            return self.event_loop.run_until_complete(self.aiterator.__anext__())
        except StopAsyncIteration as e:
            raise StopIteration() from e


async_to_sync_iter: Callable[
    [asyncio.AbstractEventLoop, AsyncIterator[T]], Iterator[T]
] = AsyncToSyncIterator
