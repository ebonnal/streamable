import asyncio
from abc import ABC, abstractmethod
from collections import deque
from concurrent.futures import Future
from contextlib import suppress
from typing import Awaitable, Collection, Deque, Iterable, Iterator, Set, Sized, Type, TypeVar, cast

with suppress(ImportError):
    from streamable.util.protocols import Queue

T = TypeVar("T")


class FutureResultCollection(Iterator[T], Sized, ABC):
    """
    Iterator over added futures' results. Supports adding new futures after iteration started.
    """
    _futures: Collection["Future[T]"]

    @abstractmethod
    def add(self, future: "Future[T]") -> None: ...

    def num_pending(self) -> int:
        return sum(1 for future in self._futures if not future.done())
    
    def pending_percent(self) -> float:
        return self.num_pending() / len(self)

    def __len__(self) -> int:
        return len(self._futures)

class DequeFutureResultCollection(FutureResultCollection[T]):
    def __init__(self) -> None:
        self._futures: Deque["Future[T]"] = deque()

    def add(self, future: "Future[T]") -> None:
        return self._futures.append(future)


class CallbackFutureResultCollection(FutureResultCollection[T]):
    def __init__(self) -> None:
        self._futures: Set["Future[T]"] = set()

    @abstractmethod
    def _done_callback(self, future: "Future[T]") -> None: ...

    def add(self, future: "Future[T]") -> None:
        future.add_done_callback(self._done_callback)
        self._futures.add(future)


class FIFOOSFutureResultCollection(DequeFutureResultCollection[T]):
    """
    First In First Out
    """

    def __next__(self) -> T:
        return self._futures.popleft().result()


class FDFOOSFutureResultCollection(CallbackFutureResultCollection[T]):
    """
    First Done First Out
    """

    def __init__(self, queue_type: Type["Queue"]) -> None:
        super().__init__()
        self._results: "Queue[T]" = queue_type()

    def _done_callback(self, future: "Future[T]") -> None:
        self._results.put(future.result())
        self._futures.remove(future)

    def __next__(self) -> T:
        return self._results.get()


class FIFOAsyncFutureResultCollection(DequeFutureResultCollection[T]):
    """
    First In First Out
    """

    def __init__(self, event_loop: asyncio.AbstractEventLoop) -> None:
        super().__init__()
        self.event_loop = event_loop

    def __next__(self) -> T:
        return self.event_loop.run_until_complete(
            cast(Awaitable[T], self._futures.popleft())
        )


class FDFOAsyncFutureResultCollection(CallbackFutureResultCollection[T]):
    """
    First Done First Out
    """

    def __init__(self, event_loop: asyncio.AbstractEventLoop) -> None:
        super().__init__()
        self.event_loop = event_loop
        self._waiter: asyncio.futures.Future[T] = self.event_loop.create_future()

    def _done_callback(self, future: "Future[T]") -> None:
        self._waiter.set_result(future.result())
        self._futures.remove(future)

    def __next__(self) -> T:
        result = self.event_loop.run_until_complete(self._waiter)
        self._waiter = self.event_loop.create_future()
        return result
