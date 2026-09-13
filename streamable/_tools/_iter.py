import asyncio
from contextlib import suppress
from typing import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Generic,
    Iterable,
    Iterator,
    Optional,
    Protocol,
    TypeVar,
    Union,
    runtime_checkable,
)


T = TypeVar("T")
U = TypeVar("U")


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


class AsyncToSyncIterator(Iterator[T]):
    __slots__ = ("iterator", "_loop")

    def __init__(self, iterator: AsyncIterator[T]):
        self.iterator = iterator
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _lazy_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            with suppress(RuntimeError):
                self._loop = asyncio.get_event_loop()
            if not self._loop or self._loop.is_closed():
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)
        return self._loop

    def __next__(self) -> T:
        try:
            return self._lazy_loop().run_until_complete(self.iterator.__anext__())
        except StopAsyncIteration:
            raise StopIteration


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

    def __init__(self, fn: Callable[[], Coroutine[object, object, T]]) -> None:
        self.fn = fn

    async def __anext__(self) -> T:
        return await self.fn()


def afn_to_aiter(fn: Callable[[], Coroutine[object, object, T]]) -> AsyncIterator[T]:
    return _AsyncFnAsyncIterator(fn)


class _FnAsyncIterator(AsyncIterator[T]):
    __slots__ = ("fn",)

    def __init__(self, fn: Callable[[], T]) -> None:
        self.fn = fn

    async def __anext__(self) -> T:
        return self.fn()


def fn_to_aiter(fn: Callable[[], T]) -> AsyncIterator[T]:
    return _FnAsyncIterator(fn)


C = TypeVar("C", covariant=True)


@runtime_checkable
class AsyncCloseable(Protocol):
    """
    Object that can be closed asynchronously.
    Any task created within the scope of this object should be cancelled when it is closed.
    """

    def aclose(self) -> Awaitable[None]: ...


@runtime_checkable
class CloseableAsyncIterator(AsyncCloseable, Protocol[C]):
    """
    An ``AsyncIterator`` that can be ``.aclose``d.
    """

    def __aiter__(self) -> AsyncIterator[C]:
        return self

    def __anext__(self) -> Awaitable[C]: ...


@runtime_checkable
class CloseableAsyncIterable(AsyncCloseable, Protocol[C]):
    def __aiter__(self) -> AsyncIterator[C]: ...


class CloseableWithUpstream(AsyncCloseable, Generic[T]):
    """
    Closeable object that propagates the closing to the upstream.
    """

    __slots__ = ("upstream",)

    def __init__(self, upstream: CloseableAsyncIterator[T]) -> None:
        self.upstream = upstream

    async def aclose(self) -> None:
        await self.upstream.aclose()


class NoopCloseableAsyncIterator(CloseableAsyncIterator[T]):
    """
    Closeable async iterator that does nothing when closed.
    """

    __slots__ = ("iterator",)

    def __init__(self, iterator: AsyncIterator[T]) -> None:
        self.iterator = iterator

    def __anext__(self) -> Awaitable[T]:
        return self.iterator.__anext__()

    async def aclose(self) -> None:
        pass
