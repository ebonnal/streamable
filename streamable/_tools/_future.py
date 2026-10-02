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
        self.futures.pop(future, None)
        return result


class FDFOFutureResults(FutureResults[T]):
    """
    First Done First Out
    """

    __slots__ = ("_done_futures",)

    def __init__(self) -> None:
        super().__init__()
        self._done_futures: "Queue[Future[T]]" = Queue()

    def __len__(self) -> int:
        return self._done_futures.qsize() + len(self.futures)

    def _done_callback(self, future: "Future[T]") -> None:
        self._done_futures.put_nowait(future)
        self.futures.pop(future, None)

    def add(self, future: "Future[T]") -> None:
        super().add(future)
        future.add_done_callback(self._done_callback)

    def __next__(self) -> T:
        return self._done_futures.get().result()
