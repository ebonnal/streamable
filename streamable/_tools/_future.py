import asyncio
from abc import abstractmethod
from collections import deque
from concurrent.futures import Future
from queue import Queue
import sys
from typing import (
    Awaitable,
    Deque,
    Generator,
    Iterator,
    Sized,
    TypeVar,
    Union,
    cast,
)


T = TypeVar("T")


class FutureResult(Future, Awaitable[T]):
    __slots__ = ()

    def __init__(self, result: T):
        super().__init__()
        self.set_result(result)

    def __await__(self) -> Generator[None, None, T]:
        yield
        return self.result()


class FutureResultCollection(Iterator[T], Sized):
    """
    Iterator over added futures' results. Supports adding new futures after iteration started.
    """

    __slots__ = ()

    @abstractmethod
    def add(self, future: "Future[T]") -> None: ...


class FIFOFutureResultCollection(FutureResultCollection[T]):
    __slots__ = ("_futures",)

    def __init__(self) -> None:
        self._futures: Deque["Future[T]"] = deque()

    def __len__(self) -> int:
        return len(self._futures)

    def add(self, future: "Future[T]") -> None:
        return self._futures.append(future)


class FDFOFutureResultCollection(FutureResultCollection[T]):
    __slots__ = ("_results", "_n_futures")
    _results: "Union[Queue[T], asyncio.Queue[T]]"

    def __init__(self) -> None:
        self._n_futures = 0

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

    __slots__ = ()

    def __next__(self) -> T:
        return self._futures.popleft().result()


class ExecutorFDFOFutureResultCollection(FDFOFutureResultCollection[T]):
    """
    First Done First Out
    """

    __slots__ = ()

    def __init__(self) -> None:
        super().__init__()
        self._results: "Queue[T]" = Queue()

    def __next__(self) -> T:
        result = self._results.get()
        self._n_futures -= 1
        return result


class AsyncFIFOFutureResultCollection(FIFOFutureResultCollection[T]):
    """
    First In First Out
    """

    __slots__ = ("loop",)

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        super().__init__()
        self.loop = loop

    def __next__(self) -> T:
        return self.loop.run_until_complete(cast(Awaitable[T], self._futures.popleft()))


class AsyncFDFOFutureResultCollection(FDFOFutureResultCollection[T]):
    """
    First Done First Out
    """

    __slots__ = ("loop",)

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
