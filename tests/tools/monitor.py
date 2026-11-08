import asyncio
import sys
import time
from typing import (
    AsyncIterable,
    AsyncIterator,
    Callable,
    Generic,
    Iterable,
    Iterator,
    List,
    TypeVar,
    Union,
)

from tests.tools.error import TestError
from tests.tools.iter import IterableType

T = TypeVar("T")

SLOW = 0.1


class MonitoredIdentity(Generic[T]):
    """
    Provides a sync and an async function recording the fate of their calls (started/cancelled/completed).
    The calls get slow after the first `n_fast` calls.
    """

    def __init__(self, n_fast: int = 1) -> None:
        self.n_fast = n_fast
        self.started: List[T] = []
        self.completed: List[T] = []
        self.cancelled: List[T] = []

    async def async_func(self, arg: T) -> T:
        self.started.append(arg)
        try:
            if len(self.started) > self.n_fast:
                await asyncio.sleep(SLOW)
        except asyncio.CancelledError:
            self.cancelled.append(arg)
            raise
        self.completed.append(arg)
        return arg

    def sync_func(self, arg: T) -> T:
        self.started.append(arg)
        if len(self.started) > self.n_fast:
            time.sleep(SLOW)
        self.completed.append(arg)
        return arg

    def func(self, itype: IterableType) -> Callable[[T], Union[T, "asyncio.Future[T]"]]:
        """The async function for an async iteration, the sync one otherwise."""
        if itype == "async":
            return self.async_func  # type: ignore[return-value]
        return self.sync_func


class MonitoredIntIterator(MonitoredIdentity[int], Iterator[int], AsyncIterator[int]):
    """Iterator over 0, 1, 2, ..., both sync and async, recording its pulls and closes."""

    n_closes = 0
    # pulls neither completed nor cancelled when closed
    n_pulls_in_flight_at_close = 0

    def __init__(self, n_fast: int = 1, n_yields: int = sys.maxsize) -> None:
        super().__init__(n_fast=n_fast)
        self.n_yields = n_yields

    @property
    def closed(self) -> bool:
        return self.n_closes > 0

    def __next__(self) -> int:
        if len(self.started) >= self.n_yields:
            raise StopIteration
        return self.sync_func(len(self.started))

    async def __anext__(self) -> int:
        if len(self.started) >= self.n_yields:
            raise StopAsyncIteration
        return await self.async_func(len(self.started))

    def _record_close(self) -> None:
        self.n_closes += 1
        self.n_pulls_in_flight_at_close = (
            len(self.started) - len(self.completed) - len(self.cancelled)
        )

    def close(self) -> None:
        self._record_close()

    async def aclose(self) -> None:
        self._record_close()


class MonitoredIntIterable(Iterable[int], AsyncIterable[int]):
    """Iterable creating (and recording) a new `MonitoredIntIterator` on each `__iter__`/`__aiter__`."""

    def __init__(self, n_fast: int = 1, n_yields: int = sys.maxsize) -> None:
        self.n_fast = n_fast
        self.n_yields = n_yields
        self.iterators: List[MonitoredIntIterator] = []

    def _new_iterator(self) -> MonitoredIntIterator:
        iterator = MonitoredIntIterator(self.n_fast, self.n_yields)
        self.iterators.append(iterator)
        return iterator

    def __iter__(self) -> MonitoredIntIterator:
        return self._new_iterator()

    def __aiter__(self) -> MonitoredIntIterator:
        return self._new_iterator()


class CloseFailingMonitoredIntIterator(MonitoredIntIterator):
    def close(self) -> None:
        super().close()
        raise TestError()

    async def aclose(self) -> None:
        await super().aclose()
        raise TestError()


class CloseFailingMonitoredIntIterable(Iterable[int], AsyncIterable[int]):
    def __init__(self, n_yields: int = sys.maxsize) -> None:
        self.n_yields = n_yields
        self.iterators: List[CloseFailingMonitoredIntIterator] = []

    def _new_iterator(self) -> CloseFailingMonitoredIntIterator:
        iterator = CloseFailingMonitoredIntIterator(n_yields=self.n_yields)
        self.iterators.append(iterator)
        return iterator

    def __iter__(self) -> CloseFailingMonitoredIntIterator:
        return self._new_iterator()

    def __aiter__(self) -> CloseFailingMonitoredIntIterator:
        return self._new_iterator()
