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

from streamable._tools._error import ExceptionContainer, RaisingIterator

T = TypeVar("T")


class SyncAsyncIterable(Iterable[T], AsyncIterable[T]):
    """Both sync and async iterable."""


class SyncToAsyncIterator(AsyncIterator[T]):
    __slots__ = ("iterator",)

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


class AsyncToSyncIterable(Iterable[Union[T, ExceptionContainer]]):
    __slots__ = "iterator"

    def __init__(self, iterator: AsyncIterator[T]):
        self.iterator = iterator

    def __iter__(self) -> Iterator[Union[T, ExceptionContainer]]:
        loop = asyncio.new_event_loop()
        try:
            while True:
                try:
                    elem = loop.run_until_complete(self.iterator.__anext__())
                except StopAsyncIteration:
                    break
                except Exception as e:
                    yield ExceptionContainer(e)
                else:
                    yield elem
        finally:
            loop.stop()
            loop.close()


class AsyncToSyncIterator(RaisingIterator[T]):
    __slots__ = "iterator"

    def __init__(self, iterator: AsyncIterator[T]):
        super().__init__(AsyncToSyncIterable(iterator).__iter__())


class _FnIterator(Iterator[T]):
    __slots__ = ("fn",)

    def __init__(self, fn: Callable[[], T]) -> None:
        self.fn = fn

    def __next__(self) -> T:
        return self.fn()


def fn_to_iter(fn: Callable[[], T]) -> Iterator[T]:
    return _FnIterator(fn)


class _AsyncFnAsyncIterator(AsyncIterator[T]):
    __slots__ = ("fn",)

    def __init__(self, fn: Callable[[], Coroutine[Any, Any, T]]) -> None:
        self.fn = fn

    async def __anext__(self) -> T:
        return await self.fn()


def afn_to_aiter(fn: Callable[[], Coroutine[Any, Any, T]]) -> AsyncIterator[T]:
    return _AsyncFnAsyncIterator(fn)


class _FnAsyncIterator(AsyncIterator[T]):
    __slots__ = ("fn",)

    def __init__(self, fn: Callable[[], T]) -> None:
        self.fn = fn

    async def __anext__(self) -> T:
        return self.fn()


def fn_to_aiter(fn: Callable[[], T]) -> AsyncIterator[T]:
    return _FnAsyncIterator(fn)
