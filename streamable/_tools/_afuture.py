import asyncio
from asyncio import Future
from abc import abstractmethod
from collections import deque
from typing import (
    AsyncIterator,
    Deque,
    Sized,
    TypeVar,
)

T = TypeVar("T")


class FutureResult(Future[T]):
    def __init__(self, result: T):
        super().__init__()
        self.set_result(result)


class FutureResultCollection(AsyncIterator[T], Sized):
    """
    Iterator over added futures' results. Supports adding new futures after iteration started.
    """

    @abstractmethod
    def add(self, future: Future[T]) -> None: ...


class FIFOFutureResultCollection(FutureResultCollection[T]):
    """
    First In First Out
    """

    def __init__(self) -> None:
        self._futures: Deque[Future[T]] = deque()

    def __len__(self) -> int:
        return len(self._futures)

    def add(self, future: Future[T]) -> None:
        return self._futures.append(future)

    async def __anext__(self) -> T:
        return await self._futures.popleft()


class FDFOFutureResultCollection(FutureResultCollection[T]):
    """
    First Done First Out
    """

    def __init__(self) -> None:
        self._results: "asyncio.Queue[T]" = asyncio.Queue()
        self._n_futures = 0

    def __len__(self) -> int:
        return self._n_futures

    def _done_callback(self, future: Future[T]) -> None:
        self._results.put_nowait(future.result())

    def add(self, future: Future[T]) -> None:
        future.add_done_callback(self._done_callback)
        self._n_futures += 1

    async def __anext__(self) -> T:
        result = await self._results.get()
        self._n_futures -= 1
        return result
