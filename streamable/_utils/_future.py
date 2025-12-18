import asyncio
from abc import ABC, abstractmethod
from collections import deque
from concurrent.futures import Future
from contextlib import suppress
import sys
from typing import (
    AsyncIterator,
    Awaitable,
    Deque,
    Generator,
    Iterator,
    Sized,
    Type,
    TypeVar,
    Union,
    cast,
)


with suppress(ImportError):
    from streamable._utils._queue import Queue

T = TypeVar("T")


class FutureResult(Future, Awaitable[T]):
    def __init__(self, result: T):
        super().__init__()
        self.set_result(result)

    def __await__(self) -> Generator[None, None, T]:
        yield
        return self.result()


class FutureResultCollection(Iterator[T], AsyncIterator[T], Sized, ABC):
    """
    Iterator over added futures' results. Supports adding new futures after iteration started.
    """

    @abstractmethod
    def add(self, future: "Future[T]") -> None: ...

    async def __anext__(self) -> T:
        return self.__next__()


class FIFOFutureResultCollection(FutureResultCollection[T]):
    def __init__(self) -> None:
        self._futures: Deque["Future[T]"] = deque()

    def __len__(self) -> int:
        return len(self._futures)

    def add(self, future: "Future[T]") -> None:
        return self._futures.append(future)


class FDFOFutureResultCollection(FutureResultCollection[T]):
    def __init__(self) -> None:
        self._n_futures = 0
        self._results: "Union[Queue[T], asyncio.Queue[T]]"

    def __len__(self) -> int:
        return self._n_futures

    def _done_callback(self, future: "Future[T]") -> None:
        self._results.put_nowait(future.result())

    def add(self, future: "Future[T]") -> None:
        future.add_done_callback(self._done_callback)
        self._n_futures += 1


class ExecutorFIFOFutureResultCollection(FIFOFutureResultCollection[T]):
    """
    First In First Out
    """

    def __next__(self) -> T:
        return self._futures.popleft().result()


class ExecutorFDFOFutureResultCollection(FDFOFutureResultCollection[T]):
    """
    First Done First Out
    """

    def __init__(self, queue_type: Type["Queue"]) -> None:
        super().__init__()
        self._results: "Queue[T]" = queue_type()

    def __next__(self) -> T:
        result = self._results.get()
        self._n_futures -= 1
        return result


class AsyncFIFOFutureResultCollection(FIFOFutureResultCollection[T]):
    """
    First In First Out
    """

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        super().__init__()
        self.loop = loop

    def __next__(self) -> T:
        return self.loop.run_until_complete(cast(Awaitable[T], self._futures.popleft()))

    async def __anext__(self) -> T:
        return await cast(Awaitable[T], self._futures.popleft())


class AsyncFDFOFutureResultCollection(FDFOFutureResultCollection[T]):
    """
    First Done First Out
    """

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        super().__init__()
        self.loop = loop
        self._results: "asyncio.Queue[T]"
        if sys.version_info >= (3, 10):
            self._results = asyncio.Queue()
        else:  # pragma: no cover
            self._results = asyncio.Queue(loop=loop)  # type: ignore

    def __next__(self) -> T:
        result = self.loop.run_until_complete(self._results.get())
        self._n_futures -= 1
        return result

    async def __anext__(self) -> T:
        result = await self._results.get()
        self._n_futures -= 1
        return result
