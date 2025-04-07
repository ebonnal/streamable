import asyncio
import datetime
import multiprocessing
import queue
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from concurrent.futures import Executor, Future, ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import contextmanager, suppress
from math import ceil
from typing import (
    Any,
    Callable,
    ContextManager,
    Coroutine,
    DefaultDict,
    Deque,
    Generic,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from streamable.util.functiontools import iter_wo_stopiteration, wrap_error
from streamable.util.loggertools import get_logger
from streamable.util.validationtools import (
    validate_base,
    validate_buffersize,
    validate_concurrency,
    validate_count,
    validate_errors,
    validate_group_size,
    validate_iterator,
    validate_optional_positive_interval,
)

T = TypeVar("T")
U = TypeVar("U")

from streamable.util.constants import NO_REPLACEMENT
from streamable.util.futuretools import (
    FDFOAsyncFutureResultCollection,
    FDFOOSFutureResultCollection,
    FIFOAsyncFutureResultCollection,
    FIFOOSFutureResultCollection,
    FutureResultCollection,
)

with suppress(ImportError):
    from typing import Literal


class CatchIterator(Iterator[T]):
    def __init__(
        self,
        iterator: Iterator[T],
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        when: Optional[Callable[[Exception], Any]],
        replacement: T,
        finally_raise: bool,
    ) -> None:
        validate_iterator(iterator)
        validate_errors(errors)
        self.iterator = iterator
        self.errors = errors
        self.when = wrap_error(when, StopIteration) if when else None
        self.replacement = replacement
        self.finally_raise = finally_raise
        self._to_be_finally_raised: Optional[Exception] = None

    def __next__(self) -> T:
        while True:
            try:
                return next(self.iterator)
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


class DistinctIterator(Iterator[T]):
    def __init__(
        self, iterator: Iterator[T], key: Optional[Callable[[T], Any]]
    ) -> None:
        validate_iterator(iterator)
        self.iterator = iterator
        self.key = wrap_error(key, StopIteration) if key else None
        self._already_seen: Set[Any] = set()

    def __next__(self) -> T:
        while True:
            elem = next(self.iterator)
            key = self.key(elem) if self.key else elem
            if key not in self._already_seen:
                break
        self._already_seen.add(key)
        return elem


class ConsecutiveDistinctIterator(Iterator[T]):
    def __init__(
        self, iterator: Iterator[T], key: Optional[Callable[[T], Any]]
    ) -> None:
        validate_iterator(iterator)
        self.iterator = iterator
        self.key = wrap_error(key, StopIteration) if key else None
        self._last_key: Any = object()

    def __next__(self) -> T:
        while True:
            elem = next(self.iterator)
            key = self.key(elem) if self.key else elem
            if key != self._last_key:
                break
        self._last_key = key
        return elem


class FlattenIterator(Iterator[U]):
    def __init__(self, iterator: Iterator[Iterable[U]]) -> None:
        validate_iterator(iterator)
        self.iterator = iterator
        self._current_iterator_elem: Iterator[U] = iter(tuple())

    def __next__(self) -> U:
        while True:
            try:
                return next(self._current_iterator_elem)
            except StopIteration:
                self._current_iterator_elem = iter_wo_stopiteration(next(self.iterator))


class _GroupIteratorMixin(Generic[T]):
    def __init__(
        self,
        iterator: Iterator[T],
        size: Optional[int],
        interval: Optional[datetime.timedelta],
    ) -> None:
        validate_iterator(iterator)
        validate_group_size(size)
        validate_optional_positive_interval(interval)
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


class GroupIterator(_GroupIteratorMixin[T], Iterator[List[T]]):
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
                self._current_group.append(next(self.iterator))
        except Exception as e:
            if not self._current_group:
                raise
            self._to_be_raised = e

        group, self._current_group = self._current_group, []
        self._remember_group_time()
        return group


class GroupbyIterator(_GroupIteratorMixin[T], Iterator[Tuple[U, List[T]]]):
    def __init__(
        self,
        iterator: Iterator[T],
        key: Callable[[T], U],
        size: Optional[int],
        interval: Optional[datetime.timedelta],
    ) -> None:
        super().__init__(iterator, size, interval)
        self.key = wrap_error(key, StopIteration)
        self._is_exhausted = False
        self._groups_by: DefaultDict[U, List[T]] = defaultdict(list)

    def _group_next_elem(self) -> None:
        elem = next(self.iterator)
        self._groups_by[self.key(elem)].append(elem)

    def _pop_full_group(self) -> Optional[Tuple[U, List[T]]]:
        for key, group in self._groups_by.items():
            if len(group) >= self.size:
                return key, self._groups_by.pop(key)
        return None

    def _pop_first_group(self) -> Tuple[U, List[T]]:
        first_key: U = next(iter(self._groups_by), cast(U, ...))
        return first_key, self._groups_by.pop(first_key)

    def _pop_largest_group(self) -> Tuple[U, List[T]]:
        largest_group_key: Any = next(iter(self._groups_by), ...)

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
            return next(self)

        except Exception as e:
            self._to_be_raised = e
            return next(self)


class CountSkipIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], count: int) -> None:
        validate_iterator(iterator)
        validate_count(count)
        self.iterator = iterator
        self.count = count
        self._n_skipped = 0
        self._done_skipping = False

    def __next__(self) -> T:
        if not self._done_skipping:
            while self._n_skipped < self.count:
                next(self.iterator)
                # do not count exceptions as skipped elements
                self._n_skipped += 1
            self._done_skipping = True
        return next(self.iterator)


class PredicateSkipIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], until: Callable[[T], Any]) -> None:
        validate_iterator(iterator)
        self.iterator = iterator
        self.until = wrap_error(until, StopIteration)
        self._done_skipping = False

    def __next__(self) -> T:
        elem = next(self.iterator)
        if not self._done_skipping:
            while not self.until(elem):
                elem = next(self.iterator)
            self._done_skipping = True
        return elem


class CountAndPredicateSkipIterator(Iterator[T]):
    def __init__(
        self, iterator: Iterator[T], count: int, until: Callable[[T], Any]
    ) -> None:
        validate_iterator(iterator)
        validate_count(count)
        self.iterator = iterator
        self.count = count
        self.until = wrap_error(until, StopIteration)
        self._n_skipped = 0
        self._done_skipping = False

    def __next__(self) -> T:
        elem = next(self.iterator)
        if not self._done_skipping:
            while self._n_skipped < self.count and not self.until(elem):
                elem = next(self.iterator)
                # do not count exceptions as skipped elements
                self._n_skipped += 1
            self._done_skipping = True
        return elem


class CountTruncateIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], count: int) -> None:
        validate_iterator(iterator)
        validate_count(count)
        self.iterator = iterator
        self.count = count
        self._current_count = 0

    def __next__(self) -> T:
        if self._current_count == self.count:
            raise StopIteration()
        elem = next(self.iterator)
        self._current_count += 1
        return elem


class PredicateTruncateIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], when: Callable[[T], Any]) -> None:
        validate_iterator(iterator)
        self.iterator = iterator
        self.when = wrap_error(when, StopIteration)
        self._satisfied = False

    def __next__(self) -> T:
        if self._satisfied:
            raise StopIteration()
        elem = next(self.iterator)
        if self.when(elem):
            self._satisfied = True
            raise StopIteration()
        return elem


class ObserveIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], what: str, base: int = 2) -> None:
        validate_iterator(iterator)
        validate_base(base)

        self.iterator = iterator
        self.what = what
        self.base = base

        self._n_yields = 0
        self._n_errors = 0
        self._n_nexts = 0
        self._logged_n_nexts = 0
        self._next_threshold = 0

        self._start_time = time.perf_counter()

    def _log(self) -> None:
        get_logger().info(
            "[%s %s] %s",
            f"duration={datetime.datetime.fromtimestamp(time.perf_counter()) - datetime.datetime.fromtimestamp(self._start_time)}",
            f"errors={self._n_errors}",
            f"{self._n_yields} {self.what} yielded",
        )
        self._logged_n_nexts = self._n_nexts
        self._next_threshold = self.base * self._logged_n_nexts

    def __next__(self) -> T:
        try:
            elem = next(self.iterator)
            self._n_nexts += 1
            self._n_yields += 1
            return elem
        except StopIteration:
            if self._n_nexts != self._logged_n_nexts:
                self._log()
            raise
        except Exception:
            self._n_nexts += 1
            self._n_errors += 1
            raise
        finally:
            if self._n_nexts >= self._next_threshold:
                self._log()


class YieldsPerPeriodThrottleIterator(Iterator[T]):
    def __init__(
        self,
        iterator: Iterator[T],
        max_yields: int,
        period: datetime.timedelta,
    ) -> None:
        validate_iterator(iterator)
        self.iterator = iterator
        self.max_yields = max_yields
        self._period_seconds = period.total_seconds()

        self._period_index: int = -1
        self._yields_in_period = 0

        self._offset: Optional[float] = None

    def safe_next(self) -> Tuple[Optional[T], Optional[Exception]]:
        try:
            return next(self.iterator), None
        except StopIteration:
            raise
        except Exception as e:
            return None, e

    def __next__(self) -> T:
        elem, caught_error = self.safe_next()

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
            raise caught_error
        return cast(T, elem)


class _RaisingIterator(Iterator[T]):
    class ExceptionContainer(NamedTuple):
        exception: Exception

    def __init__(
        self,
        iterator: Iterator[Union[T, ExceptionContainer]],
    ) -> None:
        self.iterator = iterator

    def __next__(self) -> T:
        elem = next(self.iterator)
        if isinstance(elem, self.ExceptionContainer):
            raise elem.exception
        return elem


class _ConcurrentMapIterable(
    Generic[T, U], ABC, Iterable[Union[U, _RaisingIterator.ExceptionContainer]]
):
    """
    Template Method Pattern:
    This abstract class's `__iter__` is a skeleton for a queue-based concurrent mapping algorithm
    that relies on abstract helper methods (`_context_manager`, `_create_future`, `_future_result_collection`)
    that must be implemented by concrete subclasses.
    """

    def __init__(
        self,
        iterator: Iterator[T],
        buffersize: int,
        ordered: bool,
    ) -> None:
        validate_iterator(iterator)
        validate_buffersize(buffersize)
        self.iterator = iterator
        self.buffersize = buffersize
        self.ordered = ordered

    def _context_manager(self) -> ContextManager:
        @contextmanager
        def dummy_context_manager_generator():
            yield

        return dummy_context_manager_generator()

    @abstractmethod
    def _launch_task(
        self, elem: T
    ) -> "Future[Union[U, _RaisingIterator.ExceptionContainer]]": ...

    # factory method
    @abstractmethod
    def _future_result_collection(
        self,
    ) -> FutureResultCollection[Union[U, _RaisingIterator.ExceptionContainer]]: ...

    def __iter__(self) -> Iterator[Union[U, _RaisingIterator.ExceptionContainer]]:
        with self._context_manager():
            future_results = self._future_result_collection()

            # queue tasks up to buffersize
            with suppress(StopIteration):
                while len(future_results) < self.buffersize:
                    future_results.add_future(self._launch_task(next(self.iterator)))

            # wait, queue, yield
            while future_results:
                result = next(future_results)
                with suppress(StopIteration):
                    future_results.add_future(self._launch_task(next(self.iterator)))
                yield result


class _OSConcurrentMapIterable(_ConcurrentMapIterable[T, U]):
    def __init__(
        self,
        iterator: Iterator[T],
        transformation: Callable[[T], U],
        concurrency: int,
        buffersize: int,
        ordered: bool,
        via: "Literal['thread', 'process']",
    ) -> None:
        super().__init__(iterator, buffersize, ordered)
        validate_concurrency(concurrency)
        self.transformation = wrap_error(transformation, StopIteration)
        self.concurrency = concurrency
        self.executor: Executor
        self.via = via

    def _context_manager(self) -> ContextManager:
        if self.via == "thread":
            self.executor = ThreadPoolExecutor(max_workers=self.concurrency)
        if self.via == "process":
            self.executor = ProcessPoolExecutor(max_workers=self.concurrency)
        return self.executor

    # picklable
    @staticmethod
    def _safe_transformation(
        transformation: Callable[[T], U], elem: T
    ) -> Union[U, _RaisingIterator.ExceptionContainer]:
        try:
            return transformation(elem)
        except Exception as e:
            return _RaisingIterator.ExceptionContainer(e)

    def _launch_task(
        self, elem: T
    ) -> "Future[Union[U, _RaisingIterator.ExceptionContainer]]":
        return self.executor.submit(
            self._safe_transformation, self.transformation, elem
        )

    def _future_result_collection(
        self,
    ) -> FutureResultCollection[Union[U, _RaisingIterator.ExceptionContainer]]:
        if self.ordered:
            return FIFOOSFutureResultCollection()
        return FDFOOSFutureResultCollection(
            multiprocessing.Queue if self.via == "process" else queue.Queue
        )


class OSConcurrentMapIterator(_RaisingIterator[U]):
    def __init__(
        self,
        iterator: Iterator[T],
        transformation: Callable[[T], U],
        concurrency: int,
        buffersize: int,
        ordered: bool,
        via: "Literal['thread', 'process']",
    ) -> None:
        super().__init__(
            iter(
                _OSConcurrentMapIterable(
                    iterator,
                    transformation,
                    concurrency,
                    buffersize,
                    ordered,
                    via,
                )
            )
        )


class _AsyncConcurrentMapIterable(_ConcurrentMapIterable[T, U]):
    def __init__(
        self,
        iterator: Iterator[T],
        transformation: Callable[[T], Coroutine[Any, Any, U]],
        buffersize: int,
        ordered: bool,
    ) -> None:
        super().__init__(iterator, buffersize, ordered)
        self.transformation = wrap_error(transformation, StopIteration)
        self.event_loop: asyncio.AbstractEventLoop
        try:
            self.event_loop = asyncio.get_event_loop()
        except RuntimeError:
            self.event_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.event_loop)

    async def _safe_transformation(
        self, elem: T
    ) -> Union[U, _RaisingIterator.ExceptionContainer]:
        try:
            coroutine = self.transformation(elem)
            if not isinstance(coroutine, Coroutine):
                raise TypeError(
                    f"`transformation` must be an async function i.e. a function returning a Coroutine but it returned a {type(coroutine)}",
                )
            return await coroutine
        except Exception as e:
            return _RaisingIterator.ExceptionContainer(e)

    def _launch_task(
        self, elem: T
    ) -> "Future[Union[U, _RaisingIterator.ExceptionContainer]]":
        return cast(
            "Future[Union[U, _RaisingIterator.ExceptionContainer]]",
            self.event_loop.create_task(self._safe_transformation(elem)),
        )

    def _future_result_collection(
        self,
    ) -> FutureResultCollection[Union[U, _RaisingIterator.ExceptionContainer]]:
        if self.ordered:
            return FIFOAsyncFutureResultCollection(self.event_loop)
        else:
            return FDFOAsyncFutureResultCollection(self.event_loop)


class AsyncConcurrentMapIterator(_RaisingIterator[U]):
    def __init__(
        self,
        iterator: Iterator[T],
        transformation: Callable[[T], Coroutine[Any, Any, U]],
        buffersize: int,
        ordered: bool,
    ) -> None:
        super().__init__(
            iter(
                _AsyncConcurrentMapIterable(
                    iterator,
                    transformation,
                    buffersize,
                    ordered,
                )
            )
        )


class _ConcurrentFlattenIterable(
    Iterable[Union[T, _RaisingIterator.ExceptionContainer]]
):
    def __init__(
        self,
        iterables_iterator: Iterator[Iterable[T]],
        concurrency: int,
        buffersize: int,
    ) -> None:
        validate_iterator(iterables_iterator)
        validate_concurrency(concurrency)
        self.iterables_iterator = iterables_iterator
        self.concurrency = concurrency
        self.buffersize = buffersize

    def __iter__(self) -> Iterator[Union[T, _RaisingIterator.ExceptionContainer]]:
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            iterator_and_future_pairs: Deque[Tuple[Iterator[T], Future]] = deque()
            element_to_yield: Deque[Union[T, _RaisingIterator.ExceptionContainer]] = (
                deque(maxlen=1)
            )
            iterator_to_queue: Optional[Iterator[T]] = None
            # wait, queue, yield (FIFO)
            while True:
                if iterator_and_future_pairs:
                    iterator, future = iterator_and_future_pairs.popleft()
                    try:
                        element_to_yield.append(future.result())
                        iterator_to_queue = iterator
                    except StopIteration:
                        pass
                    except Exception as e:
                        element_to_yield.append(_RaisingIterator.ExceptionContainer(e))
                        iterator_to_queue = iterator

                # queue tasks up to buffersize
                while len(iterator_and_future_pairs) < self.buffersize:
                    if not iterator_to_queue:
                        try:
                            iterable = next(self.iterables_iterator)
                        except StopIteration:
                            break
                        try:
                            iterator_to_queue = iter_wo_stopiteration(iterable)
                        except Exception as e:
                            yield _RaisingIterator.ExceptionContainer(e)
                            continue
                    future = executor.submit(
                        cast(Callable[[Iterable[T]], T], next), iterator_to_queue
                    )
                    iterator_and_future_pairs.append((iterator_to_queue, future))
                    iterator_to_queue = None
                if element_to_yield:
                    yield element_to_yield.pop()
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
            iter(
                _ConcurrentFlattenIterable(
                    iterables_iterator,
                    concurrency,
                    buffersize,
                )
            )
        )
