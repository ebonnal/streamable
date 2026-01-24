import asyncio
from asyncio import Future
from abc import abstractmethod
from collections import deque
from typing import (
    AsyncIterator,
    Deque,
    Optional,
    Sized,
    TypeVar,
)

T = TypeVar("T")


class FutureResult(Future):
    __slots__ = ()

    def __init__(self, result: T):
        super().__init__()
        self.set_result(result)


class FutureResults(AsyncIterator[T], Sized):
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

    async def __anext__(self) -> T:
        return await self._futures.popleft()


class FDFOFutureResults(FutureResults[T]):
    """
    First Done First Out
    """

    __slots__ = ("_results", "_n_futures")

    def __init__(self) -> None:
        self._results: "Optional[asyncio.Queue[T]]" = None
        self._n_futures = 0

    @property
    def _lazy_results(self) -> "asyncio.Queue[T]":
        if self._results is None:
            self._results = asyncio.Queue()
        return self._results

    def __len__(self) -> int:
        return self._n_futures

    def _done_callback(self, future: "Future[T]") -> None:
        self._lazy_results.put_nowait(future.result())

    def add(self, future: "Future[T]") -> None:
        future.add_done_callback(self._done_callback)
        self._n_futures += 1

    async def __anext__(self) -> T:
        result = await self._lazy_results.get()
        self._n_futures -= 1
        return result
