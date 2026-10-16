import asyncio
from contextlib import suppress
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


class AsyncToSyncIterator(Iterator[T]):
    __slots__ = ("upstream", "_loop")

    def __init__(self, upstream: "ClosableAsyncIterator[T]"):
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

    def close(self) -> None:
        self._lazy_loop().run_until_complete(self.upstream.aclose())

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
