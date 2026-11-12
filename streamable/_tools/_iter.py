import asyncio
import itertools
from typing import (
    Any,
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

from streamable._tools._afuture import ensure_done
from streamable._tools._async import safe_get_event_loop
from streamable._tools._func import anoop, noop

T = TypeVar("T")
U = TypeVar("U")
C = TypeVar("C", covariant=True)


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
        "_closed",
        "_stopped",
        "_pending_pull",
    )

    def __init__(self, upstream: "AsyncIterator[T]", owned: bool = True):
        self.upstream = upstream
        self._upstream_owned = owned
        self._loop_owned = False
        self.__loop: Optional[asyncio.AbstractEventLoop] = None
        self._closed = False
        self._stopped = False
        self._pending_pull: Optional["asyncio.Task[T]"] = None

    @property
    def _lazy_loop(self) -> asyncio.AbstractEventLoop:
        if self.__loop is None:
            self.__loop = safe_get_event_loop()
            if not self.__loop or self.__loop.is_closed():
                self.__loop = asyncio.new_event_loop()
                self._loop_owned = True
        return self.__loop

    async def _safe_aclose_upstream(self) -> None:
        if (
            not self._stopped
            and self._upstream_owned
            and isinstance(self.upstream, AsyncClosable)
        ):
            await ensure_done(self._pending_pull)
            await self.upstream.aclose()

    def _safe_close_loop(self) -> None:
        if self.__loop is not None and not self.__loop.is_closed() and self._loop_owned:
            self.__loop.close()

    def close(self) -> None:
        if not self._closed:
            if self.__loop is not None and not self.__loop.is_closed():
                if self.__loop.is_running():
                    raise RuntimeError("event loop already running: cannot close")
                try:
                    self.__loop.run_until_complete(self._safe_aclose_upstream())
                finally:
                    self._safe_close_loop()
            self._closed = True

    def __del__(self) -> None:
        loop = self.__loop
        if self._closed or loop is None or loop.is_closed():
            return
        if loop.is_running():
            # non-owned `self.__loop` is running (in this or another thread), schedule the aclose
            loop.call_soon_threadsafe(
                asyncio.ensure_future, self._safe_aclose_upstream()
            )
        elif not asyncio._get_running_loop():
            # no running loop in this thread, and `self.__loop` not running: can use `self._loop.run_until_complete`
            self.close()
        elif self._loop_owned:
            # `self.__loop` is owned and another loop is running in this thread
            self._safe_close_loop()

    def _next_pull(self) -> "asyncio.Task[T]":
        return asyncio.ensure_future(self.upstream.__anext__(), loop=self._lazy_loop)

    def _cancel_and_wait_pending_pull(self) -> None:
        if self._pending_pull is not None and not self._pending_pull.done():
            self._pending_pull.cancel()
            self._lazy_loop.run_until_complete(
                asyncio.gather(self._pending_pull, return_exceptions=True)
            )

    def __next__(self) -> T:
        if self._stopped or self._closed:
            raise StopIteration
        if self._lazy_loop.is_running():
            raise RuntimeError(
                "event loop already running: do an async iteration instead"
            )
        if self._pending_pull is None:
            self._pending_pull = self._next_pull()
        try:
            return self._lazy_loop.run_until_complete(self._pending_pull)
        except StopAsyncIteration:
            self._stopped = True
            self._safe_close_loop()
            raise StopIteration
        except Exception:
            raise
        except BaseException:
            self._cancel_and_wait_pending_pull()
            raise
        finally:
            if self._pending_pull.done():
                self._pending_pull = None


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


class SyncToClosableAsyncIterator(ClosableAsyncIterator[T]):
    __slots__ = ("upstream", "_upstream_owned")

    def __init__(self, upstream: Iterator[T], owned: bool = True):
        self.upstream = upstream
        self._upstream_owned = owned

    async def __anext__(self) -> T:
        try:
            return self.upstream.__next__()
        except StopIteration as e:
            raise StopAsyncIteration from e

    async def aclose(self) -> None:
        if self._upstream_owned:
            safe_close_fn(self.upstream)()


class SyncToClosableAsyncIterable(AsyncIterable[T]):
    __slots__ = ("upstream",)

    def __init__(self, upstream: Iterable[T]):
        self.upstream = upstream

    def __aiter__(self) -> ClosableAsyncIterator[T]:
        return SyncToClosableAsyncIterator(iter(self.upstream))


class GuardedAsyncIterator(ClosableAsyncIterator[T]):
    """
    Wraps an `AsyncIterator` with an `.aclose` that calls upstream's `.aclose` only if owned (i.e. if upstream iterator has been created by this wrapper).
    If `silent_close`, an `Exception` raised by upstream's `.aclose` is suppressed.
    """

    __slots__ = (
        "silent_close",
        "iterator",
        "safe_aclose_iterator",
        "_closed",
    )

    def __init__(self, iterable: AsyncIterable[T], silent_close: bool = False) -> None:
        self.silent_close = silent_close
        self.iterator = iterable.__aiter__()
        self.safe_aclose_iterator = (
            anoop
            if isinstance(iterable, AsyncIterator)
            else safe_aclose_fn(self.iterator)
        )
        self._closed = False

    def __anext__(self) -> Awaitable[T]:
        return self.iterator.__anext__()

    async def aclose(self) -> None:
        if not self._closed:
            try:
                await self.safe_aclose_iterator()
            except Exception:
                if not self.silent_close:
                    raise
            self._closed = True


class GuardedIterator(itertools.chain, ClosableIterator[T]):
    __slots__ = (
        "silent_close",
        "iterator",
        "safe_close_iterator",
        "_closed",
    )

    iterator: Iterator[T]
    safe_close_iterator: Callable[[], None]

    def __new__(
        cls, iterable: Iterable[T], silent_close: bool = False
    ) -> "GuardedIterator[T]":
        iterator = iterable.__iter__()
        self = super().__new__(cls, iterator)
        self.iterator = iterator
        self.safe_close_iterator = (
            noop if isinstance(iterable, Iterator) else safe_close_fn(self.iterator)
        )
        return self

    def __init__(self, iterable: Iterable[T], silent_close: bool = False) -> None:
        self.silent_close = silent_close
        self._closed = False

    def close(self) -> None:
        if not self._closed:
            try:
                self.safe_close_iterator()
            except Exception:
                if not self.silent_close:
                    raise
            self._closed = True


def safe_close_fn(obj: object) -> Callable[[], None]:
    def wrap() -> None:
        if isinstance(obj, Closable):
            obj.close()

    return wrap


def safe_aclose_fn(obj: object) -> Callable[[], Coroutine[None, None, None]]:
    async def wrap() -> None:
        if isinstance(obj, AsyncClosable):
            await obj.aclose()

    return wrap


class _EmptyAsyncIterator(AsyncIterator[Any]):
    __slots__ = ()

    async def __anext__(self) -> Any:
        raise StopAsyncIteration


empty_aiter: AsyncIterator[Any] = _EmptyAsyncIterator()
