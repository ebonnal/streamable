import asyncio
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Coroutine,
    Iterable,
    Iterator,
    TypeVar,
)

from streamable._utils._async import CloseEventLoopMixin

T = TypeVar("T")


class SyncAsyncIterable(Iterable[T], AsyncIterable[T]):
    pass


class SyncAsyncIterator(Iterator[T], AsyncIterator[T]):
    pass


class SyncToBiIterable(SyncAsyncIterable[T]):
    def __init__(self, iterable: Iterable[T]):
        self.iterable = iterable

    def __iter__(self) -> Iterator[T]:
        return self.iterable.__iter__()

    def __aiter__(self) -> AsyncIterator[T]:
        return SyncToAsyncIterator(self.iterable.__iter__())


sync_to_bi_iterable: Callable[[Iterable[T]], SyncAsyncIterable[T]] = SyncToBiIterable


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
        loop: asyncio.AbstractEventLoop,
        aiterator: AsyncIterator[T],
    ):
        self.aiterator = aiterator
        self.loop = loop

    def __next__(self) -> T:
        try:
            return self.loop.run_until_complete(self.aiterator.__anext__())
        except StopAsyncIteration as e:
            raise StopIteration() from e


async_to_sync_iter: Callable[
    [asyncio.AbstractEventLoop, AsyncIterator[T]], Iterator[T]
] = AsyncToSyncIterator


class _FnIterator(Iterator[T]):
    def __init__(self, fn: Callable[[], T]) -> None:
        self.fn = fn

    def __next__(self) -> T:
        return self.fn()


def fn_to_iter(fn: Callable[[], T]) -> Iterator[T]:
    return _FnIterator(fn)


class _AsyncFnIterator(Iterator[T]):
    def __init__(
        self, loop: asyncio.AbstractEventLoop, fn: Callable[[], Coroutine[Any, Any, T]]
    ) -> None:
        self.loop = loop
        self.fn = fn

    def __next__(self) -> T:
        return self.loop.run_until_complete(self.fn())


def afn_to_iter(
    loop: asyncio.AbstractEventLoop, fn: Callable[[], Coroutine[Any, Any, T]]
) -> Iterator[T]:
    return _AsyncFnIterator(loop, fn)


class _AsyncFnAsyncIterator(AsyncIterator[T]):
    def __init__(self, fn: Callable[[], Coroutine[Any, Any, T]]) -> None:
        self.fn = fn

    async def __anext__(self) -> T:
        return await self.fn()


def afn_to_aiter(fn: Callable[[], Coroutine[Any, Any, T]]) -> AsyncIterator[T]:
    return _AsyncFnAsyncIterator(fn)


class _FnAsyncIterator(AsyncIterator[T]):
    def __init__(self, fn: Callable[[], T]) -> None:
        self.fn = fn

    async def __anext__(self) -> T:
        return self.fn()


def fn_to_aiter(fn: Callable[[], T]) -> AsyncIterator[T]:
    return _FnAsyncIterator(fn)
