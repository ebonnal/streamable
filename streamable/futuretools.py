import asyncio
from abc import ABC, abstractmethod
from asyncio import AbstractEventLoop
from collections import deque
from concurrent.futures import Future
from multiprocessing import Queue
from typing import Deque, Iterator, Sized, TypeVar

T = TypeVar("T")


class FutureResultCollection(Iterator[T], Sized, ABC):
    """
    Iterator over added futures' results. Supports adding new futures after iteration started.
    """

    @abstractmethod
    def add_future(self, future: "Future[T]") -> None: ...


class DequeFutureResultCollection(FutureResultCollection[T]):
    def __init__(self) -> None:
        self._futures: Deque["Future[T]"] = deque()

    def __len__(self) -> int:
        return len(self._futures)

    def add_future(self, future: "Future[T]") -> None:
        return self._futures.append(future)


class CallbackFutureResultCollection(FutureResultCollection[T]):
    def __init__(self) -> None:
        self._n_futures = 0

    def __len__(self) -> int:
        return self._n_futures

    @abstractmethod
    def _done_callback(self, future: "Future[T]") -> None: ...

    def add_future(self, future: "Future[T]") -> None:
        future.add_done_callback(self._done_callback)
        self._n_futures += 1


class FIFOThreadFutureResultCollection(DequeFutureResultCollection[T]):
    """
    First In First Out
    """

    def __next__(self) -> T:
        return self._futures.popleft().result()


class FDFOThreadFutureResultCollection(CallbackFutureResultCollection[T]):
    """
    First Done First Out
    """

    def __init__(self) -> None:
        super().__init__()
        self._results: "Queue[T]" = Queue()

    def _done_callback(self, future: "Future[T]") -> None:
        self._results.put(future.result())

    def __next__(self) -> T:
        self._n_futures -= 1
        return self._results.get()


class FIFOAsyncFutureResultCollection(DequeFutureResultCollection[T]):
    """
    First In First Out
    """

    def __init__(self, loop: AbstractEventLoop) -> None:
        super().__init__()
        self._loop = loop

    def __next__(self) -> T:
        return self._loop.run_until_complete(self._futures.popleft())  # type: ignore


class FDFOAsyncFutureResultCollection(CallbackFutureResultCollection[T]):
    """
    First Done First Out
    """

    def __init__(self, loop: AbstractEventLoop) -> None:
        super().__init__()
        self._loop = loop
        self._waiter: asyncio.futures.Future[T] = self._loop.create_future()

    def _done_callback(self, future: "Future[T]") -> None:
        self._waiter.set_result(future.result())

    def __next__(self) -> T:
        self._n_futures -= 1
        result = self._loop.run_until_complete(self._waiter)
        self._waiter = self._loop.create_future()
        return result
