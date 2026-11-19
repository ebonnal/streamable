from abc import abstractmethod
from concurrent import futures
from concurrent.futures import Future, wait
from queue import Queue
from threading import Event, Lock
from typing import (
    Dict,
    Iterable,
    Iterator,
    Optional,
    Sized,
    TypeVar,
    Union,
)

T = TypeVar("T")


def ensure_done(
    fs: Union[Future, Iterable[Future]],
    cancel: bool = False,
    return_when: str = futures.ALL_COMPLETED,
) -> None:
    if isinstance(fs, Future):
        fs = (fs,)
    else:
        fs = tuple(fs)
    if cancel:
        for fut in fs:
            fut.cancel()
    if return_when == futures.FIRST_COMPLETED and any(fut.done() for fut in fs):
        return
    if not all(fut.done() for fut in fs):
        futures.wait(fs, return_when=return_when)


class FailedFuture(Future):
    __slots__ = ()

    def __init__(self, exception: Exception):
        super().__init__()
        self.set_exception(exception)


class FutureResults(Iterator[T], Sized):
    """
    Iterator over added futures' results. Supports adding new futures after iteration started.
    """

    __slots__ = ("event", "_futures")

    def __init__(self, event: Optional[Event] = None) -> None:
        self.event: Optional[Event] = event
        self._futures: Dict["Future[T]", object] = {}

    @abstractmethod
    def next_is_ready(self) -> bool: ...

    def add(self, future: "Future[T]") -> None:
        self._futures[future] = None
        if self.event is not None:
            event = self.event
            future.add_done_callback(lambda _: event.set())

    def __len__(self) -> int:
        return len(self._futures)

    def cancel(self) -> None:
        for future in self._futures:
            future.cancel()

    def gather(self) -> None:
        wait(self._futures)

    def clear(self) -> None:
        self._futures.clear()


class FIFOFutureResults(FutureResults[T]):
    """
    First In First Out
    """

    __slots__ = ()

    def _oldest(self) -> "Future[T]":
        return next(iter(self._futures))

    def next_is_ready(self) -> bool:
        return len(self._futures) > 0 and self._oldest().done()

    def __next__(self) -> T:
        future = self._oldest()
        wait((future,))
        try:
            return future.result()
        finally:
            self._futures.pop(future, None)
            del future


class FDFOFutureResults(FutureResults[T]):
    """
    First Done First Out
    """

    __slots__ = ("_done_futures", "_cleared", "_lock")

    def __init__(self, event: Optional[Event] = None) -> None:
        super().__init__(event)
        self._done_futures: "Queue[Future[T]]" = Queue()
        # flag to ensure that done callbacks do not enqueue futures post-clear
        self._cleared: bool = False
        self._lock = Lock()

    def next_is_ready(self) -> bool:
        return any(fut.done() for fut in self._futures)

    def clear(self) -> None:
        super().clear()
        self._cleared = True
        with self._lock:
            self._done_futures.queue.clear()

    def _done_callback(self, future: "Future[T]") -> None:
        with self._lock:
            if not self._cleared:
                self._done_futures.put_nowait(future)

    def add(self, future: "Future[T]") -> None:
        super().add(future)
        future.add_done_callback(self._done_callback)

    def __next__(self) -> T:
        done_future = self._done_futures.get()
        try:
            return done_future.result()
        finally:
            self._futures.pop(done_future, None)
            del done_future
