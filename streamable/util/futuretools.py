import asyncio
from abc import ABC, abstractmethod
from collections import deque
from concurrent.futures import Future
from contextlib import suppress
from typing import Deque, Iterator, Sized, Type, TypeVar, Union

with suppress(ImportError):
    from streamable.util.protocols import Queue

T = TypeVar("T")


class FutureResultCollection(Iterator[T], Sized, ABC):
    """
    Iterator over added futures' results. Supports adding new futures after iteration started.
    """

    @abstractmethod
    def add_future(self, future: Union[asyncio.Task[T], "Future[T]"]) -> None: ...


class FIFOFutureResultCollection(FutureResultCollection[T]):
    """
    First In First Out
    """

    def __init__(self) -> None:
        self._futures: Deque[Union[asyncio.Task[T], "Future[T]"]] = deque()

    def __len__(self) -> int:
        return len(self._futures)

    def add_future(self, future: Union[asyncio.Task[T], "Future[T]"]) -> None:
        return self._futures.append(future)

    def __next__(self) -> T:
        fut = self._futures.popleft()
        if isinstance(fut, asyncio.Task):
            fut.get_loop().run_until_complete(fut)
        return fut.result()


class FDFOFutureResultCollection(FutureResultCollection[T]):
    """
    First Done First Out
    """

    def __init__(self, queue_type: Type[Queue]) -> None:
        self._n_futures = 0
        self._results: Queue[T] = queue_type()

    def __len__(self) -> int:
        return self._n_futures

    def add_future(self, future: Union[asyncio.Task[T], "Future[T]"]) -> None:
        future.add_done_callback(self._done_callback)
        self._n_futures += 1

    def _done_callback(self, future: Union[asyncio.Task[T], "Future[T]"]) -> None:
        self._results.put_nowait(future.result())

    def __next__(self) -> T:
        result = self._results.get()
        self._n_futures -= 1
        return result
