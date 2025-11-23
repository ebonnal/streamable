import asyncio
import datetime
import multiprocessing
import queue
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from concurrent.futures import Executor, Future, ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import suppress
from math import ceil
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    ContextManager,
    Coroutine,
    DefaultDict,
    Deque,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)
from streamable._utils._async import (
    CloseEventLoopMixin,
    awaitable_to_coroutine,
    empty_aiter,
)
from streamable._utils._contextmanager import noop_context_manager
from streamable._utils._error import ExceptionContainer
from streamable._utils._logging import get_logger

from streamable._utils._const import NO_REPLACEMENT
from streamable._utils._future import (
    AsyncFDFOFutureResultCollection,
    ExecutorFDFOFutureResultCollection,
    AsyncFIFOFutureResultCollection,
    ExecutorFIFOFutureResultCollection,
    FutureResult,
    FutureResultCollection,
)

with suppress(ImportError):
    from typing import Literal

T = TypeVar("T")
U = TypeVar("U")


#########
# catch #
#########


class CatchIterator(Iterator[T]):
    def __init__(
        self,
        iterator: Iterator[T],
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        when: Optional[Callable[[Exception], Any]],
        replacement: T,
        finally_raise: bool,
    ) -> None:
        self.iterator = iterator
        self.errors = errors
        self.when = when
        self.replacement = replacement
        self.finally_raise = finally_raise
        self._to_be_finally_raised: Optional[Exception] = None

    def __next__(self) -> T:
        while True:
            try:
                return self.iterator.__next__()
            except StopIteration:
                if self._to_be_finally_raised:
                    try:
                        raise self._to_be_finally_raised
                    finally:
                        self._to_be_finally_raised = None
                raise
            except self.errors as e:
                if not self.when or self.when(e):
                    if self.finally_raise and not self._to_be_finally_raised:
                        self._to_be_finally_raised = e
                    if self.replacement is not NO_REPLACEMENT:
                        return self.replacement
                    continue
                raise


############
# distinct #
############


class DistinctIterator(Iterator[T]):
    def __init__(
        self, iterator: Iterator[T], key: Optional[Callable[[T], Any]]
    ) -> None:
        self.iterator = iterator
        self.key = key
        self._already_seen: Set[Any] = set()

    def __next__(self) -> T:
        while True:
            elem = self.iterator.__next__()
            key = self.key(elem) if self.key else elem
            if key not in self._already_seen:
                break
        self._already_seen.add(key)
        return elem


class ConsecutiveDistinctIterator(Iterator[T]):
    def __init__(
        self, iterator: Iterator[T], key: Optional[Callable[[T], Any]]
    ) -> None:
        self.iterator = iterator
        self.key = key
        self._last_key: Any = object()

    def __next__(self) -> T:
        while True:
            elem = self.iterator.__next__()
            key = self.key(elem) if self.key else elem
            if key != self._last_key:
                break
        self._last_key = key
        return elem


###########
# flatten #
###########


class FlattenIterator(Iterator[U]):
    def __init__(self, iterator: Iterator[Iterable[U]]) -> None:
        self.iterator = iterator
        self._current_iterator_elem: Iterator[U] = tuple().__iter__()

    def __next__(self) -> U:
        while True:
            try:
                return self._current_iterator_elem.__next__()
            except StopIteration:
                self._current_iterator_elem = self.iterator.__next__().__iter__()


class AFlattenIterator(Iterator[U], CloseEventLoopMixin):
    def __init__(
        self,
        event_loop: asyncio.AbstractEventLoop,
        iterator: Iterator[AsyncIterable[U]],
    ) -> None:
        self.iterator = iterator
        self.event_loop = event_loop

        self._current_iterator_elem: AsyncIterator[U] = empty_aiter()

    def __next__(self) -> U:
        while True:
            try:
                return self.event_loop.run_until_complete(
                    self._current_iterator_elem.__anext__()
                )
            except StopAsyncIteration:
                self._current_iterator_elem = self.iterator.__next__().__aiter__()


#########
# group #
#########


class _BaseGroupIterator(Generic[T]):
    def __init__(
        self,
        iterator: Iterator[T],
        size: Optional[int],
        interval: Optional[datetime.timedelta],
    ) -> None:
        self.iterator = iterator
        self.size = size or cast(int, float("inf"))
        self.interval = interval
        self._interval_seconds = interval.total_seconds() if interval else float("inf")
        self._to_be_raised: Optional[Exception] = None
        self._last_group_yielded_at: float = 0

    def _interval_seconds_have_elapsed(self) -> bool:
        if not self.interval:
            return False
        return (
            time.perf_counter() - self._last_group_yielded_at
        ) >= self._interval_seconds

    def _remember_group_time(self) -> None:
        if self.interval:
            self._last_group_yielded_at = time.perf_counter()

    def _init_last_group_time(self) -> None:
        if self.interval and not self._last_group_yielded_at:
            self._last_group_yielded_at = time.perf_counter()


class GroupIterator(_BaseGroupIterator[T], Iterator[List[T]]):
    def __init__(
        self,
        iterator: Iterator[T],
        size: Optional[int],
        interval: Optional[datetime.timedelta],
    ) -> None:
        super().__init__(iterator, size, interval)
        self._current_group: List[T] = []

    def __next__(self) -> List[T]:
        self._init_last_group_time()
        if self._to_be_raised:
            try:
                raise self._to_be_raised
            finally:
                self._to_be_raised = None
        try:
            while len(self._current_group) < self.size and (
                not self._interval_seconds_have_elapsed() or not self._current_group
            ):
                self._current_group.append(self.iterator.__next__())
        except Exception as e:
            if not self._current_group:
                raise
            self._to_be_raised = e

        group, self._current_group = self._current_group, []
        self._remember_group_time()
        return group


class GroupbyIterator(_BaseGroupIterator[T], Iterator[Tuple[U, List[T]]]):
    def __init__(
        self,
        iterator: Iterator[T],
        key: Callable[[T], U],
        size: Optional[int],
        interval: Optional[datetime.timedelta],
    ) -> None:
        super().__init__(iterator, size, interval)
        self.key = key
        self._is_exhausted = False
        self._groups_by: DefaultDict[U, List[T]] = defaultdict(list)

    def _group_next_elem(self) -> None:
        elem = self.iterator.__next__()
        self._groups_by[self.key(elem)].append(elem)

    def _pop_full_group(self) -> Optional[Tuple[U, List[T]]]:
        for key, group in self._groups_by.items():
            if len(group) >= self.size:
                return key, self._groups_by.pop(key)
        return None

    def _pop_first_group(self) -> Tuple[U, List[T]]:
        first_key: U = self._groups_by.__iter__().__next__()
        return first_key, self._groups_by.pop(first_key)

    def _pop_largest_group(self) -> Tuple[U, List[T]]:
        largest_group_key: Any = self._groups_by.__iter__().__next__()

        for key, group in self._groups_by.items():
            if len(group) > len(self._groups_by[largest_group_key]):
                largest_group_key = key

        return largest_group_key, self._groups_by.pop(largest_group_key)

    def __next__(self) -> Tuple[U, List[T]]:
        self._init_last_group_time()
        if self._is_exhausted:
            if self._groups_by:
                return self._pop_first_group()
            raise StopIteration

        if self._to_be_raised:
            if self._groups_by:
                self._remember_group_time()
                return self._pop_first_group()
            try:
                raise self._to_be_raised
            finally:
                self._to_be_raised = None

        try:
            self._group_next_elem()

            full_group: Optional[Tuple[U, List[T]]] = self._pop_full_group()
            while not full_group and not self._interval_seconds_have_elapsed():
                self._group_next_elem()
                full_group = self._pop_full_group()

            self._remember_group_time()
            return full_group or self._pop_largest_group()

        except StopIteration:
            self._is_exhausted = True
            return self.__next__()

        except Exception as e:
            self._to_be_raised = e
            return self.__next__()


########
# skip #
########


class CountSkipIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], count: int) -> None:
        self.iterator = iterator
        self.count = count
        self._n_skipped = 0
        self._done_skipping = False

    def __next__(self) -> T:
        if not self._done_skipping:
            while self._n_skipped < self.count:
                self.iterator.__next__()
                # do not count exceptions as skipped elements
                self._n_skipped += 1
            self._done_skipping = True
        return self.iterator.__next__()


class PredicateSkipIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], until: Callable[[T], Any]) -> None:
        self.iterator = iterator
        self.until = until
        self._done_skipping = False

    def __next__(self) -> T:
        elem = self.iterator.__next__()
        if not self._done_skipping:
            while not self.until(elem):
                elem = self.iterator.__next__()
            self._done_skipping = True
        return elem


class CountAndPredicateSkipIterator(Iterator[T]):
    def __init__(
        self, iterator: Iterator[T], count: int, until: Callable[[T], Any]
    ) -> None:
        self.iterator = iterator
        self.count = count
        self.until = until
        self._n_skipped = 0
        self._done_skipping = False

    def __next__(self) -> T:
        elem = self.iterator.__next__()
        if not self._done_skipping:
            while self._n_skipped < self.count and not self.until(elem):
                elem = self.iterator.__next__()
                # do not count exceptions as skipped elements
                self._n_skipped += 1
            self._done_skipping = True
        return elem


############
# truncate #
############


class CountTruncateIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], count: int) -> None:
        self.iterator = iterator
        self.count = count
        self._current_count = 0

    def __next__(self) -> T:
        if self._current_count == self.count:
            raise StopIteration()
        elem = self.iterator.__next__()
        self._current_count += 1
        return elem


class PredicateTruncateIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], when: Callable[[T], Any]) -> None:
        self.iterator = iterator
        self.when = when
        self._satisfied = False

    def __next__(self) -> T:
        if self._satisfied:
            raise StopIteration()
        elem = self.iterator.__next__()
        if self.when(elem):
            self._satisfied = True
            raise StopIteration()
        return elem


###########
# observe #
###########


class ObserveIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], what: str, base: int = 2) -> None:
        self.iterator = iterator
        self.what = what
        self.base = base
        self._yields = 0
        self._errors = 0
        self._nexts_logged = 0
        self._yields_logged = 0
        self._errors_logged = 0
        self._started_time: Optional[datetime.datetime] = None
        self._logger = get_logger()
        self._format = f"[duration=%s errors=%s] %s {what} yielded"

    def _log(self) -> None:
        now = datetime.datetime.fromtimestamp(time.perf_counter())
        duration = now - cast(datetime.datetime, self._started_time)
        self._logger.info(self._format, duration, self._errors, self._yields)

    def __next__(self) -> T:
        if not self._started_time:
            self._started_time = datetime.datetime.fromtimestamp(time.perf_counter())
        try:
            elem = self.iterator.__next__()
            self._yields += 1
            if self._yields >= self.base * self._yields_logged:
                self._log()
                self._yields_logged = self._yields
                self._nexts_logged = self._yields + self._errors
            return elem
        except StopIteration:
            if self._yields + self._errors > self._nexts_logged:
                self._log()
            raise
        except Exception:
            self._errors += 1
            if self._errors >= self.base * self._errors_logged:
                self._log()
                self._errors_logged = self._errors
                self._nexts_logged = self._yields + self._errors
            raise


############
# throttle #
############


class YieldsPerPeriodThrottleIterator(Iterator[T]):
    def __init__(
        self,
        iterator: Iterator[T],
        max_yields: int,
        period: datetime.timedelta,
    ) -> None:
        self.iterator = iterator
        self.max_yields = max_yields
        self._period_seconds = period.total_seconds()
        self._period_index: int = -1
        self._yields_in_period = 0
        self._offset: Optional[float] = None

    def __next__(self) -> T:
        elem: Optional[T] = None
        caught_error: Optional[Exception] = None
        try:
            elem = self.iterator.__next__()
        except StopIteration:
            raise
        except Exception as e:
            caught_error = e

        now = time.perf_counter()
        if not self._offset:
            self._offset = now
        now -= self._offset

        num_periods = now / self._period_seconds
        period_index = int(num_periods)

        if self._period_index != period_index:
            self._period_index = period_index
            self._yields_in_period = max(0, self._yields_in_period - self.max_yields)

        if self._yields_in_period >= self.max_yields:
            time.sleep((ceil(num_periods) - num_periods) * self._period_seconds)
        self._yields_in_period += 1

        if caught_error:
            try:
                raise caught_error
            finally:
                caught_error = None
        return cast(T, elem)


class _RaisingIterator(Iterator[T]):
    def __init__(
        self,
        iterator: Iterator[Union[T, ExceptionContainer]],
    ) -> None:
        self.iterator = iterator

    def __next__(self) -> T:
        elem = self.iterator.__next__()
        if isinstance(elem, ExceptionContainer):
            try:
                raise elem.exception
            finally:
                del elem
        return elem


##################
# concurrent map #
##################


class _BaseConcurrentMapIterable(
    Generic[T, U], ABC, Iterable[Union[U, ExceptionContainer]]
):
    def __init__(
        self,
        iterator: Iterator[T],
        buffersize: int,
        ordered: bool,
    ) -> None:
        self.iterator = iterator
        self.buffersize = buffersize
        self.ordered = ordered

    def _context_manager(self) -> ContextManager:
        return noop_context_manager()

    @abstractmethod
    def _launch_task(self, elem: T) -> "Future[Union[U, ExceptionContainer]]": ...

    # factory method
    @abstractmethod
    def _future_result_collection(
        self,
    ) -> FutureResultCollection[Union[U, ExceptionContainer]]: ...

    def _next_future(
        self,
    ) -> Optional["Future[Union[U, ExceptionContainer]]"]:
        try:
            elem = self.iterator.__next__()
        except StopIteration:
            return None
        except Exception as e:
            return FutureResult(ExceptionContainer(e))
        return self._launch_task(elem)

    def __iter__(self) -> Iterator[Union[U, ExceptionContainer]]:
        with self._context_manager():
            future_results = self._future_result_collection()

            # queue tasks up to buffersize
            while len(future_results) < self.buffersize:
                future = self._next_future()
                if not future:
                    # no more tasks to queue
                    break
                future_results.add(future)

            # queue, wait, yield
            while future_results:
                future = self._next_future()
                if future:
                    future_results.add(future)
                yield future_results.__next__()


class _ConcurrentMapIterable(_BaseConcurrentMapIterable[T, U]):
    def __init__(
        self,
        iterator: Iterator[T],
        to: Callable[[T], U],
        concurrency: int,
        buffersize: int,
        ordered: bool,
        via: "Literal['thread', 'process']",
    ) -> None:
        super().__init__(iterator, buffersize, ordered)
        self.to = to
        self.concurrency = concurrency
        self.executor: Executor
        self.via = via

    def _context_manager(self) -> ContextManager:
        if self.via == "thread":
            self.executor = ThreadPoolExecutor(max_workers=self.concurrency)
        elif self.via == "process":
            self.executor = ProcessPoolExecutor(
                max_workers=self.concurrency,
                mp_context=multiprocessing.get_context("spawn"),
            )
        return self.executor

    # picklable
    @staticmethod
    def _safe_to(to: Callable[[T], U], elem: T) -> Union[U, ExceptionContainer]:
        try:
            return to(elem)
        except Exception as e:
            return ExceptionContainer(e)

    def _launch_task(self, elem: T) -> "Future[Union[U, ExceptionContainer]]":
        return self.executor.submit(self._safe_to, self.to, elem)

    def _future_result_collection(
        self,
    ) -> FutureResultCollection[Union[U, ExceptionContainer]]:
        if self.ordered:
            return ExecutorFIFOFutureResultCollection()
        return ExecutorFDFOFutureResultCollection(
            multiprocessing.Queue if self.via == "process" else queue.Queue
        )


class ConcurrentMapIterator(_RaisingIterator[U]):
    def __init__(
        self,
        iterator: Iterator[T],
        to: Callable[[T], U],
        concurrency: int,
        buffersize: int,
        ordered: bool,
        via: "Literal['thread', 'process']",
    ) -> None:
        super().__init__(
            _ConcurrentMapIterable(
                iterator,
                to,
                concurrency,
                buffersize,
                ordered,
                via,
            ).__iter__()
        )


class _ConcurrentAMapIterable(_BaseConcurrentMapIterable[T, U], CloseEventLoopMixin):
    def __init__(
        self,
        event_loop: asyncio.AbstractEventLoop,
        iterator: Iterator[T],
        to: Callable[[T], Coroutine[Any, Any, U]],
        concurrency: int,
        buffersize: int,
        ordered: bool,
    ) -> None:
        super().__init__(iterator, buffersize, ordered)
        self.to = to
        self.event_loop = event_loop
        self.concurrency = concurrency
        self._semaphore: Optional[asyncio.Semaphore] = None

    @property
    def semaphore(self) -> asyncio.Semaphore:
        if not self._semaphore:
            self._semaphore = asyncio.Semaphore(self.concurrency)
        return self._semaphore

    async def _safe_to(self, elem: T) -> Union[U, ExceptionContainer]:
        try:
            async with self.semaphore:
                return await self.to(elem)
        except Exception as e:
            return ExceptionContainer(e)

    def _launch_task(self, elem: T) -> "Future[Union[U, ExceptionContainer]]":
        return cast(
            "Future[Union[U, ExceptionContainer]]",
            self.event_loop.create_task(self._safe_to(elem)),
        )

    def _future_result_collection(
        self,
    ) -> FutureResultCollection[Union[U, ExceptionContainer]]:
        if self.ordered:
            return AsyncFIFOFutureResultCollection(self.event_loop)
        return AsyncFDFOFutureResultCollection(self.event_loop)


class ConcurrentAMapIterator(_RaisingIterator[U]):
    def __init__(
        self,
        event_loop: asyncio.AbstractEventLoop,
        iterator: Iterator[T],
        to: Callable[[T], Coroutine[Any, Any, U]],
        concurrency: int,
        buffersize: int,
        ordered: bool,
    ) -> None:
        super().__init__(
            _ConcurrentAMapIterable(
                event_loop,
                iterator,
                to,
                concurrency,
                buffersize,
                ordered,
            ).__iter__()
        )


######################
# concurrent flatten #
######################


class _ConcurrentFlattenIterable(Iterable[Union[T, ExceptionContainer]]):
    def __init__(
        self,
        iterables_iterator: Iterator[Iterable[T]],
        concurrency: int,
        buffersize: int,
    ) -> None:
        self.iterables_iterator = iterables_iterator
        self.concurrency = concurrency
        self.buffersize = buffersize
        self._next = ExceptionContainer.wrap(next)

    def __iter__(self) -> Iterator[Union[T, ExceptionContainer]]:
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            iterator_and_future_pairs: Deque[
                Tuple[
                    Optional[Iterator[T]],
                    "Future[Union[T, ExceptionContainer]]",
                ]
            ] = deque()
            to_yield: Deque[Union[T, ExceptionContainer]] = deque(maxlen=1)
            iterator_to_queue: Optional[Iterator[T]] = None
            # wait, queue, yield (FIFO)
            while True:
                if iterator_and_future_pairs:
                    iterator, future = iterator_and_future_pairs.popleft()
                    elem = future.result()
                    if not isinstance(elem, ExceptionContainer) or not isinstance(
                        elem.exception, StopIteration
                    ):
                        to_yield.append(elem)
                        iterator_to_queue = iterator

                # queue tasks up to buffersize
                while len(iterator_and_future_pairs) < self.buffersize:
                    if not iterator_to_queue:
                        try:
                            try:
                                iterable = self.iterables_iterator.__next__()
                            except StopIteration:
                                break
                            iterator_to_queue = iterable.__iter__()
                        except Exception as e:
                            iterator_to_queue = None
                            future = FutureResult(ExceptionContainer(e))
                            iterator_and_future_pairs.append(
                                (iterator_to_queue, future)
                            )
                            continue
                    future = executor.submit(self._next, iterator_to_queue)
                    iterator_and_future_pairs.append((iterator_to_queue, future))
                    iterator_to_queue = None
                if to_yield:
                    yield to_yield.pop()
                if not iterator_and_future_pairs:
                    break


class ConcurrentFlattenIterator(_RaisingIterator[T]):
    def __init__(
        self,
        iterables_iterator: Iterator[Iterable[T]],
        concurrency: int,
        buffersize: int,
    ) -> None:
        super().__init__(
            _ConcurrentFlattenIterable(
                iterables_iterator,
                concurrency,
                buffersize,
            ).__iter__()
        )


class _ConcurrentAFlattenIterable(
    Iterable[Union[T, ExceptionContainer]], CloseEventLoopMixin
):
    def __init__(
        self,
        event_loop: asyncio.AbstractEventLoop,
        iterables_iterator: Iterator[AsyncIterable[T]],
        concurrency: int,
        buffersize: int,
    ) -> None:
        self.iterables_iterator = iterables_iterator
        self.concurrency = concurrency
        self.buffersize = buffersize
        self.event_loop = event_loop

    def __iter__(self) -> Iterator[Union[T, ExceptionContainer]]:
        iterator_and_future_pairs: Deque[
            Tuple[
                Optional[AsyncIterator[T]],
                Awaitable[Union[T, ExceptionContainer]],
            ]
        ] = deque()
        to_yield: Deque[Union[T, ExceptionContainer]] = deque(maxlen=1)
        iterator_to_queue: Optional[AsyncIterator[T]] = None
        # wait, queue, yield (FIFO)
        while True:
            if iterator_and_future_pairs:
                iterator, future = iterator_and_future_pairs.popleft()
                try:
                    to_yield.append(self.event_loop.run_until_complete(future))
                    iterator_to_queue = iterator
                except StopAsyncIteration:
                    pass
                except Exception as e:
                    to_yield.append(ExceptionContainer(e))
                    iterator_to_queue = iterator

            # queue tasks up to buffersize
            while len(iterator_and_future_pairs) < self.buffersize:
                if not iterator_to_queue:
                    try:
                        try:
                            iterable = self.iterables_iterator.__next__()
                        except StopIteration:
                            break
                        iterator_to_queue = iterable.__aiter__()
                    except Exception as e:
                        iterator_to_queue = None
                        future = FutureResult(ExceptionContainer(e))
                        iterator_and_future_pairs.append((iterator_to_queue, future))
                        continue
                future = self.event_loop.create_task(
                    awaitable_to_coroutine(iterator_to_queue.__anext__())
                )
                iterator_and_future_pairs.append((iterator_to_queue, future))
                iterator_to_queue = None
            if to_yield:
                yield to_yield.pop()
            if not iterator_and_future_pairs:
                break


class ConcurrentAFlattenIterator(_RaisingIterator[T]):
    def __init__(
        self,
        event_loop: asyncio.AbstractEventLoop,
        iterables_iterator: Iterator[AsyncIterable[T]],
        concurrency: int,
        buffersize: int,
    ) -> None:
        super().__init__(
            _ConcurrentAFlattenIterable(
                event_loop,
                iterables_iterator,
                concurrency,
                buffersize,
            ).__iter__()
        )
