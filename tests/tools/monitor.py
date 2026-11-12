import asyncio
import sys
import time
from typing import (
    AsyncIterable,
    Awaitable,
    Callable,
    Generic,
    Iterable,
    List,
    TypeVar,
    Union,
)

from streamable._tools._iter import ClosableAsyncIterator, ClosableIterator
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

    def func(self, itype: IterableType) -> Callable[[T], Union[T, "Awaitable[T]"]]:
        if itype == "async":
            return self.async_func
        return self.sync_func


class _BaseMonitoredIntIterator(MonitoredIdentity[int]):
    n_closes = 0
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


class MonitoredIntIterator(_BaseMonitoredIntIterator, ClosableIterator[int]):
    def __next__(self) -> int:
        if len(self.started) >= self.n_yields:
            raise StopIteration
        return self.sync_func(len(self.started))

    def close(self) -> None:
        self._record_close()


class MonitoredIntAsyncIterator(_BaseMonitoredIntIterator, ClosableAsyncIterator[int]):
    async def __anext__(self) -> int:
        if len(self.started) >= self.n_yields:
            raise StopAsyncIteration
        return await self.async_func(len(self.started))

    async def aclose(self) -> None:
        self._record_close()


def create_monitored_int_iterator(
    itype: IterableType, n_fast: int = 1, n_yields: int = sys.maxsize
) -> Union[MonitoredIntIterator, MonitoredIntAsyncIterator]:
    if itype == "sync":
        return MonitoredIntIterator(n_fast=n_fast, n_yields=n_yields)
    return MonitoredIntAsyncIterator(n_fast=n_fast, n_yields=n_yields)


class SlowToCancelMonitoredIntAsyncIterator(MonitoredIntAsyncIterator):
    async def __anext__(self) -> int:
        try:
            return await super().__anext__()
        except asyncio.CancelledError:
            await asyncio.sleep(1)
            raise


class _BaseMonitoredIntIterable:
    def __init__(self, n_fast: int = 1, n_yields: int = sys.maxsize) -> None:
        self.n_fast = n_fast
        self.n_yields = n_yields
        self.iterators: List[_BaseMonitoredIntIterator] = []


class MonitoredIntIterable(_BaseMonitoredIntIterable, Iterable[int]):
    def __iter__(self) -> MonitoredIntIterator:
        iterator = MonitoredIntIterator(self.n_fast, self.n_yields)
        self.iterators.append(iterator)
        return iterator


class MonitoredIntAsyncIterable(_BaseMonitoredIntIterable, AsyncIterable[int]):
    def __aiter__(self) -> MonitoredIntAsyncIterator:
        iterator = MonitoredIntAsyncIterator(self.n_fast, self.n_yields)
        self.iterators.append(iterator)
        return iterator


def create_monitored_int_iterable(
    itype: IterableType, n_fast: int = 1, n_yields: int = sys.maxsize
) -> Union[MonitoredIntIterable, MonitoredIntAsyncIterable]:
    if itype == "sync":
        return MonitoredIntIterable(n_fast=n_fast, n_yields=n_yields)
    return MonitoredIntAsyncIterable(n_fast=n_fast, n_yields=n_yields)


class CloseFailingMonitoredIntIterator(MonitoredIntIterator):
    def close(self) -> None:
        super().close()
        raise TestError()


class CloseFailingMonitoredIntAsyncIterator(MonitoredIntAsyncIterator):
    async def aclose(self) -> None:
        await super().aclose()
        raise TestError()


def create_close_failing_monitored_int_iterator(
    itype: IterableType, n_yields: int = sys.maxsize
) -> Union[CloseFailingMonitoredIntIterator, CloseFailingMonitoredIntAsyncIterator]:
    if itype == "sync":
        return CloseFailingMonitoredIntIterator(n_yields=n_yields)
    return CloseFailingMonitoredIntAsyncIterator(n_yields=n_yields)


class CloseFailingMonitoredIntIterable(Iterable[int]):
    def __init__(self, n_yields: int = sys.maxsize) -> None:
        self.n_yields = n_yields
        self.iterators: List[CloseFailingMonitoredIntIterator] = []

    def __iter__(self) -> CloseFailingMonitoredIntIterator:
        iterator = CloseFailingMonitoredIntIterator(n_yields=self.n_yields)
        self.iterators.append(iterator)
        return iterator


class CloseFailingMonitoredIntAsyncIterable(AsyncIterable[int]):
    def __init__(self, n_yields: int = sys.maxsize) -> None:
        self.n_yields = n_yields
        self.iterators: List[CloseFailingMonitoredIntAsyncIterator] = []

    def __aiter__(self) -> CloseFailingMonitoredIntAsyncIterator:
        iterator = CloseFailingMonitoredIntAsyncIterator(n_yields=self.n_yields)
        self.iterators.append(iterator)
        return iterator


def create_close_failing_monitored_int_iterable(
    itype: IterableType, n_yields: int = sys.maxsize
) -> Union[CloseFailingMonitoredIntIterable, CloseFailingMonitoredIntAsyncIterable]:
    if itype == "sync":
        return CloseFailingMonitoredIntIterable(n_yields=n_yields)
    return CloseFailingMonitoredIntAsyncIterable(n_yields=n_yields)
