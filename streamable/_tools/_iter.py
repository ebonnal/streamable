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
    Union,
)

from streamable._tools._async import CloseEventLoopMixin

T = TypeVar("T")


class SyncAsyncIterable(Iterable[T], AsyncIterable[T]):
    pass


class SyncToAsyncIterator(AsyncIterator[T]):
    def __init__(self, iterator: Iterator[T]):
        self.iterator = iterator

    async def __anext__(self) -> T:
        try:
            return self.iterator.__next__()
        except StopIteration as e:
            raise StopAsyncIteration from e


def async_iter(iterator: Union[Iterable[T], AsyncIterable[T]]) -> AsyncIterator[T]:
    if isinstance(iterator, AsyncIterable):
        return iterator.__aiter__()
    return SyncToAsyncIterator(iterator.__iter__())


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
            raise StopIteration from e


def sync_iter(
    loop_getter: Callable[[], asyncio.AbstractEventLoop],
    iterator: Union[AsyncIterable[T], Iterable[T]],
) -> Iterator[T]:
    if isinstance(iterator, Iterable):
        return iterator.__iter__()
    return AsyncToSyncIterator(loop_getter(), iterator.__aiter__())


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
