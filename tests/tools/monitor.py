import asyncio
import sys
import time
from typing import AsyncIterable, AsyncIterator, Generic, List, TypeVar

from tests.tools.error import TestError

T = TypeVar("T")

SLOW = 0.1


class MonitoredIdentity(Generic[T]):
    """
    Provides an async function recording the fate of its calls (started/cancelled/completed).
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


class MonitoredIntAsyncIterator(MonitoredIdentity[int], AsyncIterator[int]):
    n_closes = 0
    # pulls neither completed nor cancelled when `aclose` was called
    n_pulls_in_flight_at_close = 0

    def __init__(self, n_fast: int = 1, n_yields: int = sys.maxsize) -> None:
        super().__init__(n_fast=n_fast)
        self.n_yields = n_yields

    @property
    def closed(self) -> bool:
        return self.n_closes > 0

    async def __anext__(self) -> int:
        if len(self.started) >= self.n_yields:
            raise StopAsyncIteration
        return await self.async_func(len(self.started))

    async def aclose(self) -> None:
        self.n_closes += 1
        self.n_pulls_in_flight_at_close = (
            len(self.started) - len(self.completed) - len(self.cancelled)
        )


class MonitoredIntAsyncIterable(AsyncIterable[int]):
    def __init__(self, n_fast: int = 1, n_yields: int = sys.maxsize) -> None:
        self.n_fast = n_fast
        self.n_yields = n_yields
        self.iterators: List[MonitoredIntAsyncIterator] = []

    def __aiter__(self) -> MonitoredIntAsyncIterator:
        iterator = MonitoredIntAsyncIterator(self.n_fast, self.n_yields)
        self.iterators.append(iterator)
        return iterator


class CloseFailingMonitoredIntAsyncIterator(MonitoredIntAsyncIterator):
    async def aclose(self) -> None:
        await super().aclose()
        raise TestError()


class CloseFailingMonitoredIntAsyncIterable(AsyncIterable[int]):
    def __init__(self, n_yields: int = sys.maxsize) -> None:
        self.n_yields = n_yields
        self.iterators: List[CloseFailingMonitoredIntAsyncIterator] = []

    def __aiter__(self) -> CloseFailingMonitoredIntAsyncIterator:
        iterator = CloseFailingMonitoredIntAsyncIterator(n_yields=self.n_yields)
        self.iterators.append(iterator)
        return iterator
