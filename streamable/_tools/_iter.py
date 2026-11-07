import asyncio
import itertools
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

from streamable._tools._async import safe_get_event_loop

T = TypeVar("T")
U = TypeVar("U")
C = TypeVar("C", covariant=True)


class SyncAsyncIterable(Iterable[T], AsyncIterable[T]):
    """Both sync and async iterable."""


@runtime_checkable
class Closable(Protocol):
    __slots__ = ()

    def close(self) -> None: ...


@runtime_checkable
class ClosableIterator(Closable, Protocol[C]):
    __slots__ = ()

    def __iter__(self) -> Iterator[C]:
        return self

    def __next__(self) -> C: ...


class AsyncToSyncIterator(Iterator[T], Closable):
    __slots__ = (
        "upstream",
        "__loop",
        "_loop_owned",
        "_upstream_owned",
        "_closure_requested",
    )

    def __init__(self, upstream: "AsyncIterator[T]", owned: bool = True):
        self.upstream = upstream
        self._upstream_owned = owned
        self._loop_owned = False
        self.__loop: Optional[asyncio.AbstractEventLoop] = None
        self._closure_requested = False

    @property
    def _loop(self) -> asyncio.AbstractEventLoop:
        if self.__loop is None:
            self.__loop = safe_get_event_loop()
            if not self.__loop or self.__loop.is_closed():
                self.__loop = asyncio.new_event_loop()
                self._loop_owned = True
        return self.__loop

    def close(self) -> None:
        if not self._closure_requested:
            self._closure_requested = True
            try:
                if self._upstream_owned and isinstance(self.upstream, AsyncClosable):
                    self._loop.run_until_complete(self.upstream.aclose())
            finally:
                if self._loop_owned:
                    self._loop.close()

    def __del__(self) -> None:
        loop = self.__loop
        if self._closure_requested or not loop or loop.is_closed():
            return
        if loop.is_running():
            # non-owned `self.__loop` is running (in this or another thread), schedule the aclose
            if self._upstream_owned and isinstance(self.upstream, AsyncClosable):
                loop.call_soon_threadsafe(asyncio.ensure_future, self.upstream.aclose())
        elif not asyncio._get_running_loop():
            # no running loop in this thread, and `self.__loop` not running: can use `self._loop.run_until_complete`
            self.close()
        elif self._loop_owned:
            # `self.__loop` is owned and another loop is running in this thread
            loop.close()

    def __next__(self) -> T:
        if self._closure_requested:
            raise StopIteration
        if self._loop.is_running():
            raise RuntimeError(
                "event loop already running: do an async iteration instead"
            )
        anext_task = asyncio.ensure_future(self.upstream.__anext__(), loop=self._loop)
        try:
            return self._loop.run_until_complete(anext_task)
        except StopAsyncIteration:
            self.close()
            raise StopIteration
        except Exception:
            raise
        except BaseException:
            if not anext_task.done():
                anext_task.cancel()
                self._loop.run_until_complete(
                    asyncio.gather(anext_task, return_exceptions=True)
                )
            raise


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


class SyncToAsyncIterator(ClosableAsyncIterator[T]):
    __slots__ = ("upstream",)

    def __init__(self, upstream: Iterator[T]):
        self.upstream = upstream

    async def __anext__(self) -> T:
        try:
            return self.upstream.__next__()
        except StopIteration as e:
            raise StopAsyncIteration from e

    async def aclose(self) -> None:
        if isinstance(self.upstream, Closable):
            self.upstream.close()


class SyncToAsyncIterable(AsyncIterable[T]):
    __slots__ = ("upstream",)

    def __init__(self, upstream: Iterable[T]):
        self.upstream = upstream

    def __aiter__(self) -> AsyncIterator[T]:
        return SyncToAsyncIterator(iter(self.upstream))


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


class GuardedIterator(itertools.chain, ClosableIterator[T]):
    __slots__ = ("silent_close", "_upstream_iterator", "_owned", "_closure_requested")

    _upstream_iterator: Iterator[T]

    def __new__(
        cls, upstream_iterable: Iterable[T], silent_close: bool = False
    ) -> "GuardedIterator[T]":
        upstream_iterator = upstream_iterable.__iter__()
        self = super().__new__(cls, upstream_iterator)
        self._upstream_iterator = upstream_iterator
        return self

    def __init__(
        self, upstream_iterable: Iterable[T], silent_close: bool = False
    ) -> None:
        self.silent_close = silent_close
        self._owned = not isinstance(upstream_iterable, Iterator)
        self._closure_requested = False

    def close(self) -> None:
        if not self._closure_requested:
            self._closure_requested = True
            if self._owned and isinstance(self._upstream_iterator, Closable):
                try:
                    self._upstream_iterator.close()
                except Exception:
                    if not self.silent_close:
                        raise
