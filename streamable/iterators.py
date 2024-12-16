import asyncio
import datetime
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

from streamable.util.functiontools import noop_stopiteration
from streamable.util.loggertools import get_logger
from streamable.util.validationtools import (
    validate_buffersize,
    validate_concurrency,
    validate_count,
    validate_group_interval,
    validate_group_size,
    validate_iterator,
    validate_throttle_interval,
)

T = TypeVar("T")
U = TypeVar("U")

from streamable.util.constants import NO_REPLACEMENT
from streamable.util.futuretools import (
    FDFOAsyncFutureResultCollection,
    FDFOThreadFutureResultCollection,
    FIFOAsyncFutureResultCollection,
    FIFOThreadFutureResultCollection,
    FutureResultCollection,
)

with suppress(ImportError):
    from typing import Literal


class CatchIterator(Iterator[T]):
    def __init__(
        self,
        iterator: Iterator[T],
        kind: Type[Exception],
        when: Callable[[Exception], Any],
        replacement: T,
        finally_raise: bool,
    ) -> None:
        validate_iterator(iterator)
        self.iterator = iterator
        self.kind = kind
        self.when = noop_stopiteration(when)
        self.replacement = replacement
        self.finally_raise = finally_raise
        self._to_be_finally_raised: Optional[Exception] = None

    def __next__(self) -> T:
        while True:
            try:
                return next(self.iterator)
            except StopIteration:
                if self.finally_raise and self._to_be_finally_raised:
                    exception = self._to_be_finally_raised
                    self._to_be_finally_raised = None
                    raise exception
                raise
            except Exception as exception:
                if isinstance(exception, self.kind) and self.when(exception):
                    if self._to_be_finally_raised is None:
                        self._to_be_finally_raised = exception
                    if self.replacement is not NO_REPLACEMENT:
                        return self.replacement
                    continue
                raise


class DistinctIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], by: Optional[Callable[[T], Any]]) -> None:
        validate_iterator(iterator)
        self.iterator = iterator
        self.by = noop_stopiteration(by) if by else None
        self._already_seen_set: Set[Any] = set()
        self._already_seen_list: List[Any] = list()

    def _value(self, elem):
        return self.by(elem) if self.by else elem

    def _see(self, elem: Any):
        value = self._value(elem)
        try:
            value = hash(value)
        except TypeError:
            self._already_seen_list.append(value)
        else:
            self._already_seen_set.add(value)

    def _has_been_seen(self, elem: Any):
        value = self._value(elem)
        try:
            value = hash(value)
        except TypeError:
            return value in self._already_seen_list
        return value in self._already_seen_set

    def __next__(self) -> T:
        elem = next(self.iterator)
        while self._has_been_seen(elem):
            elem = next(self.iterator)
        self._see(elem)
        return elem


class ConsecutiveDistinctIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], by: Optional[Callable[[T], Any]]) -> None:
        validate_iterator(iterator)
        self.iterator = iterator
        self.by = noop_stopiteration(by) if by else None
        self._has_yielded = False
        self._last_value: Any = None

    def _next_elem_and_value(self) -> Tuple[T, Any]:
        elem = next(self.iterator)
        value = self.by(elem) if self.by else elem
        return elem, value

    def __next__(self) -> T:
        elem, value = self._next_elem_and_value()
        while self._has_yielded and value == self._last_value:
            elem, value = self._next_elem_and_value()
        self._has_yielded = True
        self._last_value = value
        return elem


class FlattenIterator(Iterator[U]):
    def __init__(self, iterator: Iterator[Iterable[U]]) -> None:
        validate_iterator(iterator)
        self.iterator = iterator
        self._current_iterator_elem: Iterator[U] = iter([])

    def __next__(self) -> U:
        while True:
            try:
                return next(self._current_iterator_elem)
            except StopIteration:
                iterable_elem = next(self.iterator)
                self._current_iterator_elem = noop_stopiteration(iter)(iterable_elem)


class _GroupIteratorInitMixin(Generic[T]):
    def __init__(
        self,
        iterator: Iterator[T],
        size: Optional[int],
        interval: Optional[datetime.timedelta],
    ) -> None:
        validate_iterator(iterator)
        validate_group_size(size)
        validate_group_interval(interval)
        self.iterator = iterator
        self.size = size if size else cast(int, float("inf"))
        self._interval_seconds = (
            float("inf") if interval is None else interval.total_seconds()
        )
        self._to_be_raised: Optional[Exception] = None
        self._last_group_yielded_at: float = 0


class GroupIterator(_GroupIteratorInitMixin[T], Iterator[List[T]]):
    def __init__(
        self,
        iterator: Iterator[T],
        size: Optional[int],
        interval: Optional[datetime.timedelta],
    ) -> None:
        super().__init__(iterator, size, interval)
        self._current_group: List[T] = []

    def _interval_seconds_have_elapsed(self) -> bool:
        return (time.time() - self._last_group_yielded_at) >= self._interval_seconds

    def __next__(self) -> List[T]:
        if not self._last_group_yielded_at:
            self._last_group_yielded_at = time.time()
        if self._to_be_raised:
            e, self._to_be_raised = self._to_be_raised, None
            raise e
        try:
            while len(self._current_group) < self.size and (
                not self._interval_seconds_have_elapsed() or not self._current_group
            ):
                self._current_group.append(next(self.iterator))
        except Exception as e:
            if not self._current_group:
                raise e
            self._to_be_raised = e

        group, self._current_group = self._current_group, []
        self._last_group_yielded_at = time.time()
        return group


class GroupbyIterator(_GroupIteratorInitMixin[T], Iterator[Tuple[U, List[T]]]):
    def __init__(
        self,
        iterator: Iterator[T],
        by: Callable[[T], U],
        size: Optional[int],
        interval: Optional[datetime.timedelta],
    ) -> None:
        super().__init__(iterator, size, interval)
        self.by = noop_stopiteration(by)
        self._is_exhausted = False
        self._groups_by: DefaultDict[U, List[T]] = defaultdict(list)

    def _interval_seconds_have_elapsed(self) -> bool:
        return (time.time() - self._last_group_yielded_at) >= self._interval_seconds

    def _group_next_elem(self) -> None:
        elem = next(self.iterator)
        self._groups_by[self.by(elem)].append(elem)

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

    def _return_group(self, group: Tuple[U, List[T]]) -> Tuple[U, List[T]]:
        self._last_group_yielded_at = time.time()
        return group

    def __next__(self) -> Tuple[U, List[T]]:
        if not self._last_group_yielded_at:
            self._last_group_yielded_at = time.time()
        if self._is_exhausted:
            if self._groups_by:
                return self._return_group(self._pop_first_group())
            raise StopIteration

        if self._to_be_raised:
            if self._groups_by:
                return self._return_group(self._pop_first_group())
            e = self._to_be_raised
            self._to_be_raised = None
            raise e

        try:
            self._group_next_elem()

            full_group: Optional[Tuple[U, List[T]]] = self._pop_full_group()
            while not full_group and not self._interval_seconds_have_elapsed():
                self._group_next_elem()
                full_group = self._pop_full_group()

            if full_group:
                return self._return_group(full_group)
            return self._return_group(self._pop_largest_group())

        except StopIteration:
            self._is_exhausted = True
            return next(self)

        except Exception as e:
            self._to_be_raised = e
            return next(self)


class SkipIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], count: int) -> None:
        validate_iterator(iterator)
        validate_count(count)
        self.iterator = iterator
        self.count = count
        self._skipped = 0

    def __next__(self):
        while self._skipped < self.count:
            next(self.iterator)
            # do not count exceptions as skipped elements
            self._skipped += 1
        return next(self.iterator)


class CountTruncateIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], count: int) -> None:
        validate_iterator(iterator)
        validate_count(count)
        self.iterator = iterator
        self.count = count
        self._current_count = 0

    def __next__(self):
        if self._current_count == self.count:
            raise StopIteration()
        elem = next(self.iterator)
        self._current_count += 1
        return elem


class PredicateTruncateIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], when: Callable[[T], Any]) -> None:
        validate_iterator(iterator)
        self.iterator = iterator
        self.when = noop_stopiteration(when)
        self._satisfied = False

    def __next__(self):
        if self._satisfied:
            raise StopIteration()
        elem = next(self.iterator)
        if self.when(elem):
            self._satisfied = True
            return next(self)
        return elem


class ObserveIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], what: str) -> None:
        validate_iterator(iterator)
        self.iterator = iterator
        self.what = what
        self._n_yields = 0
        self._n_errors = 0
        self._logged_n_calls = 0
        self._start_time = time.time()

    def _log(self) -> None:
        get_logger().info(
            "[%s %s] %s",
            f"duration={datetime.datetime.fromtimestamp(time.time()) - datetime.datetime.fromtimestamp(self._start_time)}",
            f"errors={self._n_errors}",
            f"{self._n_yields} {self.what} yielded",
        )
        self._logged_n_calls = self._n_calls()

    def _n_calls(self) -> int:
        return self._n_yields + self._n_errors

    def __next__(self) -> T:
        try:
            elem = next(self.iterator)
            self._n_yields += 1
            return elem
        except StopIteration:
            if self._n_calls() != self._logged_n_calls:
                self._log()
            raise
        except Exception as e:
            self._n_errors += 1
            raise e
        finally:
            if self._n_calls() >= 2 * self._logged_n_calls:
                self._log()


class IntervalThrottleIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], interval: datetime.timedelta) -> None:
        validate_iterator(iterator)
        validate_throttle_interval(interval)
        self.iterator = iterator
        self._interval_seconds = interval.total_seconds()

    def __next__(self) -> T:
        start_time = time.time()
        elem = next(self.iterator)
        elapsed_time = time.time() - start_time
        if self._interval_seconds > elapsed_time:
            time.sleep(self._interval_seconds - elapsed_time)
        return elem


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

    def __next__(self) -> T:
        current_time = time.time()
        if not self._offset:
            self._offset = current_time
        current_time -= self._offset

        num_periods = current_time / self._period_seconds
        period_index = int(num_periods)

        if self._period_index != period_index:
            self._period_index = period_index
            self._yields_in_period = 0

        if self._yields_in_period >= self.max_yields:
            time.sleep((ceil(num_periods) - num_periods) * self._period_seconds)
            return next(self)

        # yield
        self._yields_in_period += 1
        return next(self.iterator)


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
        self.transformation = noop_stopiteration(transformation)
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
            return FIFOThreadFutureResultCollection()
        else:
            return FDFOThreadFutureResultCollection()


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
        self.transformation = noop_stopiteration(transformation)

    async def _safe_transformation(
        self, elem: T
    ) -> Union[U, _RaisingIterator.ExceptionContainer]:
        try:
            coroutine = self.transformation(elem)
            if not isinstance(coroutine, Coroutine):
                raise TypeError(
                    f"The function is expected to be an async function, i.e. it must be a function returning a Coroutine object, but returned a {type(coroutine)}."
                )
            return await coroutine
        except Exception as e:
            return _RaisingIterator.ExceptionContainer(e)

    def _launch_task(
        self, elem: T
    ) -> "Future[Union[U, _RaisingIterator.ExceptionContainer]]":
        return cast(
            "Future[Union[U, _RaisingIterator.ExceptionContainer]]",
            asyncio.get_event_loop().create_task(self._safe_transformation(elem)),
        )

    def _future_result_collection(
        self,
    ) -> FutureResultCollection[Union[U, _RaisingIterator.ExceptionContainer]]:
        if self.ordered:
            return FIFOAsyncFutureResultCollection()
        else:
            return FDFOAsyncFutureResultCollection()


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
            iterator_to_queue: Deque[Iterator[T]] = deque(maxlen=1)
            # wait, queue, yield (FIFO)
            while True:
                if iterator_and_future_pairs:
                    iterator, future = iterator_and_future_pairs.popleft()
                    try:
                        element_to_yield.append(future.result())
                        iterator_to_queue.append(iterator)
                    except StopIteration:
                        pass
                    except Exception as e:
                        element_to_yield.append(_RaisingIterator.ExceptionContainer(e))
                        iterator_to_queue.append(iterator)

                # queue tasks up to buffersize
                while len(iterator_and_future_pairs) < self.buffersize:
                    if iterator_to_queue:
                        iterator = iterator_to_queue.pop()
                    else:
                        try:
                            iterable = next(self.iterables_iterator)
                        except StopIteration:
                            break
                        try:
                            iterator = noop_stopiteration(iter)(iterable)
                        except Exception as e:
                            yield _RaisingIterator.ExceptionContainer(e)
                            continue
                    future = executor.submit(
                        cast(Callable[[Iterable[T]], T], next), iterator
                    )
                    iterator_and_future_pairs.append((iterator, future))
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
