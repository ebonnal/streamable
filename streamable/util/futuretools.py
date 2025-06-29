from abc import ABC, abstractmethod
import asyncio
from collections import deque
from concurrent.futures import Future
from contextlib import suppress
from typing import Deque, Iterator, Optional, Sized, Type, TypeVar

with suppress(ImportError):
    from streamable.util.protocols import Queue

T = TypeVar("T")


class TaskToFuture(Future[T]):
    def __init__(self, task: asyncio.Task[T]) -> None:
        self.task = task

    def result(self, timeout: Optional[float] = None) -> T:
        if timeout:
            raise NotImplementedError("the timeout parameter is not supported")
        return self.task.get_loop().run_until_complete(self.task)


class FutureResultCollection(Iterator[T], Sized, ABC):
    """
    Iterator over added futures' results. Supports adding new futures after iteration started.
    """

    @abstractmethod
    def add_future(self, future: "Future[T]") -> None: ...


class FIFOFutureResultCollection(FutureResultCollection[T]):
    """
    First In First Out
    """

    def __init__(self) -> None:
        self._futures: Deque["Future[T]"] = deque()

    def __len__(self) -> int:
        return len(self._futures)

    def add_future(self, future: "Future[T]") -> None:
        return self._futures.append(future)

    def __next__(self) -> T:
        return self._futures.popleft().result()


class FDFOFutureResultCollection(FutureResultCollection[T]):
    """
    First Done First Out
    """

    def __init__(self, queue_type: Type["Queue"]) -> None:
        self._n_futures = 0
        self._results: "Queue[T]" = queue_type()

    def __len__(self) -> int:
        return self._n_futures

    def add_future(self, future: "Future[T]") -> None:
        future.add_done_callback(self._done_callback)
        self._n_futures += 1

    def _done_callback(self, future: "Future[T]") -> None:
        self._results.put_nowait(future.result())

    def __next__(self) -> T:
        result = self._results.get()
        self._n_futures -= 1
        return result
