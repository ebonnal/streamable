import asyncio
from concurrent import futures
from contextlib import suppress
from typing import (
    AsyncIterator,
    Dict,
    Iterable,
    Optional,
    Sized,
    TypeVar,
    Union,
)

T = TypeVar("T")


class FailedFuture(asyncio.Future):
    __slots__ = ()

    def __init__(self, exception: Exception):
        super().__init__()
        self.set_exception(exception)


def safe_wrap_future(
    future: "Union[asyncio.Future[T], futures.Future[T]]",
) -> "asyncio.Future[T]":
    wrapped = asyncio.wrap_future(future)
    if wrapped is not future:
        # prevent "Future exception was never retrieved" warnings
        wrapped.add_done_callback(
            lambda f: f.exception() if not f.cancelled() else None
        )
    return wrapped


def cancel_threadsafe(
    futures_: "Iterable[Union[asyncio.Future[T], futures.Future[T]]]",
) -> None:
    for future in futures_:
        if isinstance(future, futures.Future):
            future.cancel()
        elif not future.done():
            with suppress(RuntimeError):
                future.get_loop().call_soon_threadsafe(future.cancel)


async def lazy_wait(
    fs: Union[
        Union[asyncio.Future, futures.Future, None],
        Iterable[Union[asyncio.Future, futures.Future, None]],
    ],
    timeout: "Optional[float]" = None,
    cancel: bool = False,
) -> bool:
    """Returns True if all done"""
    if fs is None:
        return True
    if isinstance(fs, (asyncio.Future, futures.Future)):
        fs = (fs,)
    else:
        fs = tuple(fs)
    if cancel:
        for future in fs:
            if future is not None and not future.done():
                future.cancel()
    futures_to_wait = [
        safe_wrap_future(future)
        for future in fs
        if future is not None and not future.done()
    ]
    if futures_to_wait:
        return not (await asyncio.wait(futures_to_wait, timeout=timeout))[1]
    return True


class FutureResults(AsyncIterator[T], Sized):
    """
    Iterator over added futures' results. Supports adding new futures after iteration started.
    """

    __slots__ = ("_futures",)

    def __init__(self) -> None:
        self._futures: Dict["Union[asyncio.Future[T], futures.Future[T]]", object] = {}

    def add(self, future: "Union[asyncio.Future[T], futures.Future[T]]") -> None:
        self._futures[future] = None

    def __len__(self) -> int:
        return len(self._futures)

    def cancel(self) -> None:
        for future in self._futures:
            future.cancel()

    def cancel_threadsafe(self) -> None:
        cancel_threadsafe(self._futures)

    async def gather(self) -> None:
        await lazy_wait(self._futures)

    def clear(self) -> None:
        self._futures.clear()


class FIFOFutureResults(FutureResults[T]):
    """
    First In First Out
    """

    __slots__ = ()

    async def __anext__(self) -> T:
        future = next(iter(self._futures))
        if not future.done():
            await lazy_wait(future)
        try:
            return future.result()
        finally:
            self._futures.pop(future, None)
            del future


class FDFOFutureResults(FutureResults[T]):
    """
    First Done First Out
    """

    __slots__ = ("_done_futures", "_cleared")

    def __init__(self) -> None:
        super().__init__()
        self._done_futures: "asyncio.Queue[Union[asyncio.Future[T], futures.Future[T]]]" = asyncio.Queue()
        # flag to ensure that done callbacks do not enqueue futures post-clear
        self._cleared = False

    def clear(self) -> None:
        super().clear()
        self._cleared = True
        while not self._done_futures.empty():
            self._done_futures.get_nowait()

    def _done_callback(
        self, future: "Union[asyncio.Future[T], futures.Future[T]]"
    ) -> None:
        if not self._cleared:
            self._done_futures.put_nowait(future)

    def add(self, future: "Union[asyncio.Future[T], futures.Future[T]]") -> None:
        super().add(future)
        if isinstance(future, futures.Future):
            loop = asyncio.get_running_loop()
            future.add_done_callback(
                lambda future: loop.call_soon_threadsafe(self._done_callback, future)
            )
        else:
            future.add_done_callback(self._done_callback)

    async def __anext__(self) -> T:
        done_future = await self._done_futures.get()
        try:
            return done_future.result()
        finally:
            self._futures.pop(done_future, None)
            del done_future
