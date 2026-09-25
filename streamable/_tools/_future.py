from concurrent.futures import Future
from queue import Queue
from typing import (
    Dict,
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

    __slots__ = ("futures",)

    def __init__(self) -> None:
        self.futures: Dict["Future[T]", object] = {}

    def add(self, future: "Future[T]") -> None:
        self.futures[future] = None


class FIFOFutureResults(FutureResults[T]):
    """
    First In First Out
    """

    def __len__(self) -> int:
        return len(self.futures)

    def __next__(self) -> T:
        future = next(iter(self.futures))
        result = future.result()
        del self.futures[future]
        return result


class FDFOFutureResults(FutureResults[T]):
    """
    First Done First Out
    """

    __slots__ = ("_results",)

    def __init__(self) -> None:
        super().__init__()
        self._results: "Queue[T]" = Queue()

    def __len__(self) -> int:
        return self._results.qsize() + len(self.futures)

    def _done_callback(self, future: "Future[T]") -> None:
        if not future.cancelled():
            self._results.put_nowait(future.result())
        del self.futures[future]

    def add(self, future: "Future[T]") -> None:
        super().add(future)
        future.add_done_callback(self._done_callback)

    def __next__(self) -> T:
        return self._results.get()
