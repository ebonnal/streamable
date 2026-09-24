from abc import abstractmethod
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
C = TypeVar("C", covariant=True)


class SyncAsyncIterable(Iterable[T], AsyncIterable[T]):
    """Both sync and async iterable."""


class SyncToAsyncIterator(AsyncIterator[T]):
    __slots__ = ("upstream",)

    def __init__(self, upstream: Iterator[T]):
        self.upstream = upstream

    async def __anext__(self) -> T:
        try:
            return self.upstream.__next__()
        except StopIteration as e:
            raise StopAsyncIteration from e


def async_iter(iterator: Union[Iterable[T], AsyncIterable[T]]) -> AsyncIterator[T]:
    if isinstance(iterator, AsyncIterable):
        return iterator.__aiter__()
    return SyncToAsyncIterator(iterator.__iter__())


class AsyncToSyncIterator(Iterator[T]):
    __slots__ = ("upstream", "_loop")

    def __init__(self, upstream: AsyncIterator[T]):
        self.upstream = upstream
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
            return self._lazy_loop().run_until_complete(self.upstream.__anext__())
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


@runtime_checkable
class AsyncClosable(Protocol):
    def aclose(self) -> Awaitable[None]: ...


@runtime_checkable
class ClosableAsyncIterator(AsyncClosable, Protocol[C]):
    """
    An ``AsyncIterator`` with an `.aclose` method to eagerly cancel any pending child tasks spawned during its iteration.
    """

    def __aiter__(self) -> AsyncIterator[C]:
        return self

    def __anext__(self) -> Awaitable[C]: ...


class NoopClosableAsyncIterator(ClosableAsyncIterator[T]):
    """
    AsyncIterator wrapper to make it closable, with an `.aclose` that does nothing.
    """

    __slots__ = ("upstream",)

    def __init__(self, upstream: AsyncIterator[T]) -> None:
        self.upstream = upstream

    def __anext__(self) -> Awaitable[T]:
        return self.upstream.__anext__()

    async def aclose(self) -> None:
        pass


class ClosableAsyncIteratorWithUpstream(ClosableAsyncIterator[U], Generic[T, U]):
    """
    AsyncIterator wrapper to make it closable, with an `.aclose` that closes the upstream.
    Closes when iterator is exhausted.
    Ensures `.__anext__` raises StopAsyncIteration if closed.
    """

    __slots__ = ("upstream", "_closed")

    def __init__(self, upstream: ClosableAsyncIterator[T]) -> None:
        self.upstream = upstream
        self._closed = False

    @abstractmethod
    def _anext(self) -> Awaitable[U]: ...

    async def __anext__(self) -> U:
        if self._closed:
            raise StopAsyncIteration
        try:
            return await self._anext()
        except StopAsyncIteration:
            await self.aclose()
            self._closed = True
            raise

    async def aclose(self) -> None:
        if not self._closed:
            self._closed = True
            await self.upstream.aclose()
