from concurrent.futures import Future
from queue import Queue
from typing import (
    Dict,
    Iterator,
    Sized,
    TypeVar,
    Union,
)

from streamable._tools._error import ExceptionContainer

T = TypeVar("T")


class FailedFuture(Future):
    __slots__ = ()

    def __init__(self, exception: Exception):
        super().__init__()
        self.set_exception(exception)


class FutureResults(Iterator[Union[T, ExceptionContainer]], Sized):
    """
    Iterator over added futures' results. Supports adding new futures after iteration started.
    """

    __slots__ = ("_futures",)

    def __init__(self) -> None:
        self._futures: Dict["Future[T]", object] = {}

    def add(self, future: "Future[T]") -> None:
        self._futures[future] = None

    def __len__(self) -> int:
        return len(self._futures)

    def cancel(self) -> None:
        for future in self._futures:
            future.cancel()

    def clear(self) -> None:
        self._futures.clear()


class FIFOFutureResults(FutureResults[T]):
    """
    First In First Out
    """

    def __next__(self) -> Union[T, ExceptionContainer]:
        future = next(iter(self._futures))
        try:
            return ExceptionContainer.result(future)
        finally:
            self._futures.pop(future, None)
            del future


class FDFOFutureResults(FutureResults[T]):
    """
    First Done First Out
    """

    __slots__ = ("_done_futures",)

    def __init__(self) -> None:
        super().__init__()
        self._done_futures: "Queue[Future[T]]" = Queue()

    def _done_callback(self, future: "Future[T]") -> None:
        self._done_futures.put_nowait(future)

    def clear(self) -> None:
        super().clear()
        with self._done_futures.mutex:
            self._done_futures.queue.clear()

    def add(self, future: "Future[T]") -> None:
        super().add(future)
        future.add_done_callback(self._done_callback)

    def __next__(self) -> Union[T, ExceptionContainer]:
        done_future = self._done_futures.get()
        try:
            return ExceptionContainer.result(done_future)
        finally:
            self._futures.pop(done_future, None)
            del done_future
