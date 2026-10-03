import asyncio
from asyncio import Future
from typing import (
    AsyncIterator,
    Dict,
    Sized,
    TypeVar,
    Union,
)

from streamable._tools._error import ExceptionContainer

T = TypeVar("T")


class FailedFuture(Future):
    __slots__ = ()

    def __init__(self, exception: Exception):
        super().__init__()
        self.set_exception(exception)


class FutureResults(AsyncIterator[Union[T, ExceptionContainer]], Sized):
    """
    Iterator over added futures' results. Supports adding new futures after iteration started.
    """

    __slots__ = ("_futures",)

    def __init__(self) -> None:
        self._futures: Dict["Future[T]", object] = {}

    def add(self, future: "Future[T]") -> None:
        self._futures[future] = None

    def __len__(self) -> int:
        return len(self._futures)

    async def cancel(self) -> None:
        for future in self._futures:
            future.cancel()
        await asyncio.gather(*self._futures, return_exceptions=True)

    def clear(self) -> None:
        self._futures.clear()


class FIFOFutureResults(FutureResults[T]):
    """
    First In First Out
    """

    async def __anext__(self) -> Union[T, ExceptionContainer]:
        future = next(iter(self._futures))
        try:
            return await ExceptionContainer.aresult(future)
        finally:
            self._futures.pop(future, None)
            del future


class FDFOFutureResults(FutureResults[T]):
    """
    First Done First Out
    """

    __slots__ = ("_done_futures",)

    def __init__(self) -> None:
        super().__init__()
        self._done_futures: "asyncio.Queue[Future[T]]" = asyncio.Queue()

    def _done_callback(self, future: "Future[T]") -> None:
        self._done_futures.put_nowait(future)

    def clear(self) -> None:
        super().clear()
        while not self._done_futures.empty():
            self._done_futures.get_nowait()

    def add(self, future: "Future[T]") -> None:
        super().add(future)
        future.add_done_callback(self._done_callback)

    async def __anext__(self) -> Union[T, ExceptionContainer]:
        done_future = await self._done_futures.get()
        try:
            return await ExceptionContainer.aresult(done_future)
        finally:
            self._futures.pop(done_future, None)
            del done_future
