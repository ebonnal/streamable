import asyncio
from typing import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Iterable,
    Iterator,
    Optional,
    Protocol,
    TypeVar,
    runtime_checkable,
)

from streamable._tools._async import get_event_loop_if_set

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


@runtime_checkable
class Closable(Protocol):
    __slots__ = ()

    def close(self) -> None: ...


class AsyncToSyncIterator(Iterator[T], Closable):
    __slots__ = ("upstream", "__loop", "_owned_loop", "_closure_requested")

    def __init__(self, upstream: "ClosableAsyncIterator[T]"):
        self.upstream = upstream
        self.__loop: Optional[asyncio.AbstractEventLoop] = None
        self._owned_loop = False
        self._closure_requested = False

    @property
    def _loop(self) -> asyncio.AbstractEventLoop:
        if self.__loop is None:
            self.__loop = get_event_loop_if_set()
            if not self.__loop or self.__loop.is_closed():
                self.__loop = asyncio.new_event_loop()
                self._owned_loop = True
        return self.__loop

    def close(self) -> None:
        if not self._closure_requested:
            self._closure_requested = True
            try:
                self._loop.run_until_complete(self.upstream.aclose())
            finally:
                if self._owned_loop:
                    self._loop.close()

    def __del__(self) -> None:
        if self._owned_loop:
            self._loop.close()

    def __next__(self) -> T:
        if self._closure_requested:
            raise StopIteration
        try:
            if not self._loop.is_running():
                return self._loop.run_until_complete(self.upstream.__anext__())
            raise RuntimeError(
                "event loop already running: do an async iteration instead"
            )
        except StopAsyncIteration:
            self.close()
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
        try:
            return self.fn()
        except StopIteration as e:
            raise StopAsyncIteration from e


def fn_to_aiter(fn: Callable[[], T]) -> AsyncIterator[T]:
    return _FnAsyncIterator(fn)


@runtime_checkable
class AsyncClosable(Protocol):
    __slots__ = ()

    def aclose(self) -> Awaitable[None]: ...


@runtime_checkable
class ClosableAsyncIterator(AsyncClosable, Protocol[C]):
    __slots__ = ()

    def __aiter__(self) -> AsyncIterator[C]:
        return self

    def __anext__(self) -> Awaitable[C]: ...


class GuardedAsyncIterator(ClosableAsyncIterator[T]):
    """
    Wraps an `AsyncIterator` with an `.aclose` that stops the iteration and calls upstream's `.aclose` only if owned (i.e. if upstream iterator has been created by this wrapper).
    If `silent_close`, an `Exception` raised by upstream's `.aclose` is suppressed.
    """

    __slots__ = ("silent_close", "_upstream_iterator", "_owned", "_closure_requested")

    def __init__(
        self, upstream_iterable: AsyncIterable[T], silent_close: bool = False
    ) -> None:
        self.silent_close = silent_close
        self._upstream_iterator = upstream_iterable.__aiter__()
        self._owned = not isinstance(upstream_iterable, AsyncIterator)
        self._closure_requested = False

    async def __anext__(self) -> T:
        if self._closure_requested:
            raise StopAsyncIteration
        try:
            return await self._upstream_iterator.__anext__()
        except StopAsyncIteration:
            await self.aclose()
            raise
        except (Exception, GeneratorExit):
            raise
        except BaseException:
            await self.aclose()
            raise

    async def aclose(self) -> None:
        if not self._closure_requested:
            self._closure_requested = True
            if self._owned and isinstance(self._upstream_iterator, AsyncClosable):
                try:
                    await self._upstream_iterator.aclose()
                except Exception:
                    if not self.silent_close:
                        raise
