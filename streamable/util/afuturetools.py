import asyncio
from abc import ABC, abstractmethod
from collections import deque
from contextlib import suppress
from typing import AsyncIterator, Deque, Sized, TypeVar

with suppress(ImportError):
    pass

T = TypeVar("T")


class AFutureResultCollection(AsyncIterator[T], Sized, ABC):
    """
    Iterator over added futures' results. Supports adding new futures after iteration started.
    """

    @abstractmethod
    def add_future(self, future: "asyncio.Future[T]") -> None: ...


class FIFOAFutureResultCollection(AFutureResultCollection[T]):
    """
    First In First Out
    """

    def __init__(self) -> None:
        self._futures: Deque["asyncio.Future[T]"] = deque()

    def __len__(self) -> int:
        return len(self._futures)

    def add_future(self, future: "asyncio.Future[T]") -> None:
        return self._futures.append(future)

    async def __anext__(self) -> T:
        return await self._futures.popleft()


class FDFOAFutureResultCollection(AFutureResultCollection[T]):
    """
    First Done First Out
    """

    def __init__(self) -> None:
        self._n_futures = 0
        self._results: "asyncio.Queue[T]" = asyncio.Queue()

    def __len__(self) -> int:
        return self._n_futures

    def add_future(self, future: "asyncio.Future[T]") -> None:
        future.add_done_callback(self._done_callback)
        self._n_futures += 1

    def _done_callback(self, future: "asyncio.Future[T]") -> None:
        self._results.put_nowait(future.result())

    async def __anext__(self) -> T:
        result = await self._results.get()
        self._n_futures -= 1
        return result
