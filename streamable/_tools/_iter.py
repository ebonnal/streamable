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
    Union,
    runtime_checkable,
)

from streamable._tools._afuture import cancelling_on_error, ensure_done
from streamable._tools._async import (
    complete_agen_finalizers,
    safe_get_event_loop,
)
from streamable._tools._error import ExceptionContainer
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
        "_loop",
        "_loop_owned",
        "_upstream_owned",
        "_closed",
        "_stopped",
        "_in_flight_pull",
    )

    def __init__(self, upstream: "AsyncIterator[T]", owned: bool = True):
        self.upstream = upstream
        self._upstream_owned = owned
        self._loop_owned = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._closed = False
        self._stopped = False
        self._in_flight_pull: Optional["asyncio.Task[Union[T, ExceptionContainer]]"] = (
            None
        )

    @property
    def _lazy_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            self._loop = safe_get_event_loop()
            if self._loop is None or self._loop.is_closed():
                self._loop = asyncio.new_event_loop()
                self._loop_owned = True
        return self._loop

    async def _safe_aclose_upstream(self) -> None:
        if (
            not self._stopped
            and self._upstream_owned
            and isinstance(self.upstream, AsyncClosable)
        ):
            await ensure_done(self._in_flight_pull)
            await self.upstream.aclose()

    def _safe_close_loop(self) -> None:
        loop = self._loop
        if loop is not None and not loop.is_closed() and self._loop_owned:
            try:
                if not asyncio._get_running_loop():
                    # release, so the finalizations of dropped async gens get scheduled before the loop is closed
                    self.upstream = empty_aiter
                    cancelling_on_error(loop, complete_agen_finalizers(loop))
            finally:
                loop.close()

    def close(self) -> None:
        if not self._closed:
            if self._loop is not None and not self._loop.is_closed():
                if self._loop.is_running():
                    raise RuntimeError("event loop already running: cannot close")
                ExceptionContainer.raise_if_instance(
                    cancelling_on_error(
                        self._loop,
                        ExceptionContainer.acontaining(self._safe_aclose_upstream()),
                    )
                )
                self._safe_close_loop()
            self._closed = True

    def __del__(self) -> None:
        loop = self._loop
        if self._closed or loop is None or loop.is_closed():
            return
        if loop.is_running():
            # non-owned `self._loop` is running (in this or another thread), schedule the aclose
            loop.call_soon_threadsafe(
                asyncio.ensure_future, self._safe_aclose_upstream()
            )
        elif not asyncio._get_running_loop():
            # no running loop in this thread, and `self._loop` not running: can use `self._loop.run_until_complete`
            try:
                self.close()
            except BaseException:
                self._safe_close_loop()
                raise
        elif self._loop_owned:
            # `self._loop` is owned and another loop is running in this thread
            self._safe_close_loop()

    @staticmethod
    async def _safe_pull(upstream: AsyncIterator[T]) -> Union[T, ExceptionContainer]:
        return await ExceptionContainer.acontaining(upstream.__anext__())

    def __next__(self) -> T:
        try:
            if self._stopped or self._closed:
                raise StopIteration
            if self._lazy_loop.is_running():
                raise RuntimeError(
                    "event loop already running: do an async iteration instead"
                )
            if self._in_flight_pull is None:
                self._in_flight_pull = self._lazy_loop.create_task(
                    self._safe_pull(self.upstream)
                )
            try:
                result = cancelling_on_error(self._lazy_loop, self._in_flight_pull)
            except BaseException:
                if self._in_flight_pull.done():
                    self._in_flight_pull = None
                raise
            self._in_flight_pull = None
            try:
                return ExceptionContainer.raise_if_instance(result)
            except StopAsyncIteration:
                self._stopped = True
            # outside the `except` to prevent the above `StopAsyncIteration`'s traceback
            # to keep agens referenced, and hence not finalizable by below `_safe_close_loop`
            self._safe_close_loop()
            raise StopIteration
        finally:
            del self


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
    __slots__ = ("upstream",)

    def __init__(self, upstream: Iterator[T]):
        self.upstream = upstream

    async def __anext__(self) -> T:
        try:
            return self.upstream.__next__()
        except StopIteration as e:
            raise StopAsyncIteration from e

    async def aclose(self) -> None:
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
    """

    __slots__ = (
        "iterator",
        "safe_aclose_iterator",
        "_closed",
    )

    def __init__(self, iterable: AsyncIterable[T]) -> None:
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
            await self.safe_aclose_iterator()
            self._closed = True


class GuardedIterator(itertools.chain, ClosableIterator[T]):
    __slots__ = (
        "iterator",
        "safe_close_iterator",
        "_closed",
    )

    iterator: Iterator[T]
    safe_close_iterator: Callable[[], None]
    _closed: bool

    def __new__(cls, iterable: Iterable[T]) -> "GuardedIterator[T]":
        iterator = iterable.__iter__()
        self = super().__new__(cls, iterator)
        self.iterator = iterator
        self.safe_close_iterator = (
            noop if isinstance(iterable, Iterator) else safe_close_fn(self.iterator)
        )
        self._closed = False
        return self

    def close(self) -> None:
        if not self._closed:
            self.safe_close_iterator()
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


class _EmptyIterator(Iterator[Any]):
    __slots__ = ()

    def __next__(self) -> Any:
        raise StopIteration


empty_iter: Iterator[Any] = _EmptyIterator()


class _EmptyAsyncIterator(AsyncIterator[Any]):
    __slots__ = ()

    async def __anext__(self) -> Any:
        raise StopAsyncIteration


empty_aiter: AsyncIterator[Any] = _EmptyAsyncIterator()
