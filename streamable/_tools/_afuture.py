import asyncio
from asyncio import Future
from typing import (
    AsyncIterator,
    Dict,
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

    async def __anext__(self) -> T:
        future = next(iter(self.futures))
        del self.futures[future]
        return await future


class FDFOFutureResults(FutureResults[T]):
    """
    First Done First Out
    """

    __slots__ = ("_results",)

    def __init__(self) -> None:
        super().__init__()
        self._results: "Optional[asyncio.Queue[T]]" = None

    @property
    def _lazy_results(self) -> "asyncio.Queue[T]":
        if self._results is None:
            self._results = asyncio.Queue()
        return self._results

    def __len__(self) -> int:
        return self._lazy_results.qsize() + len(self.futures)

    def _done_callback(self, future: "Future[T]") -> None:
        del self.futures[future]
        if not future.cancelled():
            self._lazy_results.put_nowait(future.result())

    def add(self, future: "Future[T]") -> None:
        super().add(future)
        future.add_done_callback(self._done_callback)

    async def __anext__(self) -> T:
        return await self._lazy_results.get()
