import asyncio
from abc import abstractmethod
from concurrent import futures
from contextlib import suppress
from typing import (
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sized,
    TypeVar,
    Union,
)

from streamable._tools._func import suppressing

T = TypeVar("T")


class Signal(asyncio.Future):
    def __init__(self) -> None:
        super().__init__(loop=asyncio.get_running_loop())
        self.futures: List[futures.Future] = []

    def __call__(self, _: futures.Future) -> None:
        with suppress(RuntimeError):
            self.get_loop().call_soon_threadsafe(lambda: self.set_result(None))

    @classmethod
    def of(
        cls,
        future: Union[asyncio.Future, futures.Future],
    ) -> "asyncio.Future[None]":
        if asyncio.isfuture(future):
            return future
        signal = cls()
        future.add_done_callback(signal)
        signal.futures.append(future)
        return signal

    def close(self) -> None:
        while self.futures:
            fut = self.futures.pop()
            with fut._condition:
                if not fut.done():
                    fut._done_callbacks.remove(self)  # type: ignore


def cancel_threadsafe(
    fs: Union[
        asyncio.Future,
        futures.Future,
        Iterable[Union[asyncio.Future, futures.Future]],
        None,
    ],
) -> None:
    if fs is None:
        return
    if isinstance(fs, (asyncio.Future, futures.Future)):
        fs = (fs,)
    else:
        fs = tuple(fs)
    for future in fs:
        if isinstance(future, futures.Future):
            future.cancel()
        elif not future.done():
            with suppress(RuntimeError):
                future.get_loop().call_soon_threadsafe(future.cancel)


async def ensure_done(
    fs: Union[
        Union[asyncio.Future, futures.Future, None],
        Iterable[Union[asyncio.Future, futures.Future]],
    ],
    timeout: "Optional[float]" = None,
    cancel: bool = False,
    return_when: str = asyncio.ALL_COMPLETED,
) -> bool:
    """Returns True if all futures completed within the timeout."""
    if fs is None:
        return True
    if isinstance(fs, (asyncio.Future, futures.Future)):
        fs = (fs,)
    else:
        fs = tuple(fs)
    if cancel:
        for fut in fs:
            fut.cancel()
    all_done = all(fut.done() for fut in fs)
    if return_when == asyncio.FIRST_COMPLETED and any(fut.done() for fut in fs):
        return all_done
    if not all_done:
        if timeout == 0:
            return False
        futures_to_wait = [Signal.of(fut) for fut in fs]
        try:
            return not (
                await asyncio.wait(
                    futures_to_wait,
                    timeout=timeout,
                    return_when=return_when,
                )
            )[1]
        finally:
            for fut in futures_to_wait:
                if isinstance(fut, Signal):
                    fut.close()
    return True


def cancelling_on_error(
    loop: asyncio.AbstractEventLoop, awaitable: "Awaitable[T]"
) -> T:
    task = asyncio.ensure_future(awaitable, loop=loop)
    try:
        return loop.run_until_complete(task)
    except BaseException:
        loop.run_until_complete(ensure_done(task, cancel=True))
        raise
    finally:
        del task


class FailedFuture(asyncio.Future):
    __slots__ = ()

    def __init__(self, exception: Exception):
        super().__init__()
        self.set_exception(exception)


class FutureResults(AsyncIterator[T], Sized):
    """
    Iterator over added futures' results. Supports adding new futures after iteration started.
    """

    __slots__ = ("event", "_futures")

    def __init__(self, event: Optional[asyncio.Event] = None) -> None:
        self.event: Optional[asyncio.Event] = event
        self._futures: Dict["Union[asyncio.Future[T], futures.Future[T]]", object] = {}

    @abstractmethod
    def next_is_ready(self) -> bool: ...

    def _add_done_callback(
        self,
        future: "Union[asyncio.Future[T], futures.Future[T]]",
        done_callback: Callable[["Union[asyncio.Future[T], futures.Future[T]]"], None],
    ) -> None:
        if isinstance(future, futures.Future):
            loop = asyncio.get_running_loop()
            future.add_done_callback(
                lambda fut: suppressing(
                    RuntimeError, loop.call_soon_threadsafe, done_callback, fut
                )
            )
        else:
            future.add_done_callback(done_callback)

    def add(self, future: "Union[asyncio.Future[T], futures.Future[T]]") -> None:
        self._futures[future] = None
        if self.event is not None:
            event = self.event
            self._add_done_callback(future, lambda _: event.set())

    def __len__(self) -> int:
        return len(self._futures)

    def cancel(self) -> None:
        for future in self._futures:
            future.cancel()

    def cancel_threadsafe(self) -> None:
        cancel_threadsafe(self._futures)

    async def gather(self) -> None:
        await ensure_done(self._futures)

    def clear(self) -> None:
        self._futures.clear()


class FIFOFutureResults(FutureResults[T]):
    """
    First In First Out
    """

    __slots__ = ()

    def _oldest(self) -> "Union[asyncio.Future[T], futures.Future[T]]":
        return next(iter(self._futures))

    def next_is_ready(self) -> bool:
        return len(self._futures) > 0 and self._oldest().done()

    async def __anext__(self) -> T:
        future = self._oldest()
        await ensure_done(future)
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

    def __init__(self, event: Optional[asyncio.Event] = None) -> None:
        super().__init__(event)
        self._done_futures: "asyncio.Queue[Union[asyncio.Future[T], futures.Future[T]]]" = asyncio.Queue()
        # flag to ensure that done callbacks do not enqueue futures post-clear
        self._cleared = False

    def next_is_ready(self) -> bool:
        return any(fut.done() for fut in self._futures)

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
        self._add_done_callback(future, self._done_callback)

    async def __anext__(self) -> T:
        done_future = await self._done_futures.get()
        try:
            return done_future.result()
        finally:
            self._futures.pop(done_future, None)
            del done_future
