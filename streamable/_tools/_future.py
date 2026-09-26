from concurrent.futures import Future
from queue import Queue
from typing import (
    Iterator,
    TypeVar,
    Union,
    cast,
)

from streamable._tools._sentinel import Sentinel


T = TypeVar("T")


class FutureResult(Future):
    __slots__ = ()

    def __init__(self, result: T):
        super().__init__()
        self.set_result(result)


class FutureResults(Iterator[T]):
    """
    Iterator over added futures' results. Supports adding new futures after iteration started.
    """

    def add(self, future: "Future[Union[T, Sentinel]]") -> None: ...


class FIFOFutureResults(FutureResults[T]):
    """
    First In First Out
    """

    __slots__ = ("_futures", "_stopped")

    def __init__(self) -> None:
        self._futures: Queue["Future[Union[T, Sentinel]]"] = Queue()
        self._stopped = False

    def add(self, future: "Future[Union[T, Sentinel]]") -> None:
        self._futures.put(future)

    def __next__(self) -> T:
        if self._stopped:
            raise StopIteration
        result = self._futures.get().result()
        if isinstance(result, Sentinel):
            self._stopped = True
            return self.__next__()
        return cast(T, result)


class FDFOFutureResults(FutureResults[T]):
    """
    First Done First Out
    """

    __slots__ = ("_results", "_n_future_results", "_stopped")

    def __init__(self) -> None:
        self._results: "Queue[Union[T, Sentinel]]" = Queue()
        self._n_future_results = 0
        self._stopped = False

    def _done_callback(self, future: "Future[Union[T, Sentinel]]") -> None:
        if not future.cancelled():
            self._results.put_nowait(future.result())

    def add(self, future: "Future[Union[T, Sentinel]]") -> None:
        self._n_future_results += 1
        future.add_done_callback(self._done_callback)

    def __next__(self) -> T:
        if self._stopped and self._n_future_results == 0:
            raise StopIteration
        result = self._results.get()
        self._n_future_results -= 1
        if isinstance(result, Sentinel):
            self._stopped = True
            return self.__next__()
        return cast(T, result)
