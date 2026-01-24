from abc import abstractmethod
from collections import deque
from concurrent.futures import Future
from queue import Queue
from typing import (
    Deque,
    Iterator,
    Sized,
    TypeVar,
)


T = TypeVar("T")


class FutureResult(Future):
    __slots__ = ()

    def __init__(self, result: T):
        super().__init__()
        self.set_result(result)


class FutureResults(Iterator[T], Sized):
    """
    Iterator over added futures' results. Supports adding new futures after iteration started.
    """

    __slots__ = ()

    @abstractmethod
    def add(self, future: "Future[T]") -> None: ...


class FIFOFutureResults(FutureResults[T]):
    """
    First In First Out
    """

    __slots__ = ("_futures",)

    def __init__(self) -> None:
        self._futures: Deque["Future[T]"] = deque()

    def __len__(self) -> int:
        return len(self._futures)

    def add(self, future: "Future[T]") -> None:
        return self._futures.append(future)

    def __next__(self) -> T:
        return self._futures.popleft().result()


class FDFOFutureResults(FutureResults[T]):
    """
    First Done First Out
    """

    __slots__ = ("_results", "_n_futures")
    _results: "Queue[T]"

    def __init__(self) -> None:
        super().__init__()
        self._results: "Queue[T]" = Queue()
        self._n_futures = 0

    def __len__(self) -> int:
        return self._n_futures

    def _done_callback(self, future: "Future[T]") -> None:
        self._results.put_nowait(future.result())

    def add(self, future: "Future[T]") -> None:
        future.add_done_callback(self._done_callback)
        self._n_futures += 1

    def __next__(self) -> T:
        result = self._results.get()
        self._n_futures -= 1
        return result
