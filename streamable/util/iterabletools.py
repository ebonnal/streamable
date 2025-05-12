from typing import (
    AsyncIterable,
    AsyncIterator,
    Callable,
    Iterable,
    Iterator,
    TypeVar,
)

from streamable.util.asynctools import GetEventLoopMixin

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
        return SyncToAsyncIterator(self.iterable)


sync_to_bi_iterable: Callable[[Iterable[T]], BiIterable[T]] = SyncToBiIterable


class SyncToAsyncIterator(AsyncIterator[T]):
    def __init__(self, iterator: Iterable[T]):
        self.iterator: Iterator[T] = iterator.__iter__()

    async def __anext__(self) -> T:
        try:
            return self.iterator.__next__()
        except StopIteration as e:
            raise StopAsyncIteration() from e


sync_to_async_iter: Callable[[Iterable[T]], AsyncIterator[T]] = SyncToAsyncIterator


class AsyncToSyncIterator(Iterator[T], GetEventLoopMixin):
    def __init__(self, iterator: AsyncIterable[T]):
        self.iterator: AsyncIterator[T] = iterator.__aiter__()

    def __next__(self) -> T:
        try:
            return self.get_event_loop().run_until_complete(self.iterator.__anext__())
        except StopAsyncIteration as e:
            raise StopIteration() from e


async_to_sync_iter: Callable[[AsyncIterable[T]], Iterator[T]] = AsyncToSyncIterator
