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
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from streamable.util.asynctools import (
    GetEventLoopMixin,
    awaitable_to_coroutine,
    empty_aiter,
)
from streamable.util.functiontools import (
    aiter_wo_stopasynciteration,
    awrap_error,
    iter_wo_stopasynciteration,
    wrap_error,
)
from streamable.util.loggertools import get_logger
from streamable.util.validationtools import (
    validate_aiterator,
    validate_base,
    validate_buffersize,
    validate_concurrency,
    validate_count,
    validate_errors,
    validate_group_size,
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


class ACatchAsyncIterator(AsyncIterator[T]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        when: Optional[Callable[[Exception], Coroutine[Any, Any, Any]]],
        replacement: T,
        finally_raise: bool,
    ) -> None:
        validate_aiterator(iterator)
        validate_errors(errors)
        self.iterator = iterator
        self.errors = errors
        self.when = awrap_error(when, StopAsyncIteration) if when else None
        self.replacement = replacement
        self.finally_raise = finally_raise
        self._to_be_finally_raised: Optional[Exception] = None

    async def __anext__(self) -> T:
        while True:
            try:
                return await self.iterator.__anext__()
            except StopAsyncIteration:
                if self._to_be_finally_raised:
                    try:
                        raise self._to_be_finally_raised
                    finally:
                        self._to_be_finally_raised = None
                raise
            except self.errors as e:
                if not self.when or await self.when(e):
                    if self.finally_raise and not self._to_be_finally_raised:
                        self._to_be_finally_raised = e
                    if self.replacement is not NO_REPLACEMENT:
                        return self.replacement
                    continue
                raise


class ADistinctAsyncIterator(AsyncIterator[T]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        key: Optional[Callable[[T], Coroutine[Any, Any, Any]]],
    ) -> None:
        validate_aiterator(iterator)
        self.iterator = iterator
        self.key = awrap_error(key, StopAsyncIteration) if key else None
        self._already_seen: Set[Any] = set()

    async def __anext__(self) -> T:
        while True:
            elem = await self.iterator.__anext__()
            key = await self.key(elem) if self.key else elem
            if key not in self._already_seen:
                break
        self._already_seen.add(key)
        return elem


class ConsecutiveADistinctAsyncIterator(AsyncIterator[T]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        key: Optional[Callable[[T], Coroutine[Any, Any, Any]]],
    ) -> None:
        validate_aiterator(iterator)
        self.iterator = iterator
        self.key = awrap_error(key, StopAsyncIteration) if key else None
        self._last_key: Any = object()

    async def __anext__(self) -> T:
        while True:
            elem = await self.iterator.__anext__()
            key = await self.key(elem) if self.key else elem
            if key != self._last_key:
                break
        self._last_key = key
        return elem


class FlattenAsyncIterator(AsyncIterator[U]):
    def __init__(self, iterator: AsyncIterator[Iterable[U]]) -> None:
        validate_aiterator(iterator)
        self.iterator = iterator
        self._current_iterator_elem: Iterator[U] = tuple().__iter__()

    async def __anext__(self) -> U:
        while True:
            try:
                return self._current_iterator_elem.__next__()
            except StopIteration:
                self._current_iterator_elem = iter_wo_stopasynciteration(
                    await self.iterator.__anext__()
                )


class AFlattenAsyncIterator(AsyncIterator[U]):
    def __init__(self, iterator: AsyncIterator[AsyncIterable[U]]) -> None:
        validate_aiterator(iterator)
        self.iterator = iterator
        self._current_iterator_elem: AsyncIterator[U] = empty_aiter()

    async def __anext__(self) -> U:
        while True:
            try:
                return await self._current_iterator_elem.__anext__()
            except StopAsyncIteration:
                self._current_iterator_elem = aiter_wo_stopasynciteration(
                    await self.iterator.__anext__()
                )


class _GroupAsyncIteratorMixin(Generic[T]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        size: Optional[int],
        interval: Optional[datetime.timedelta],
    ) -> None:
        validate_aiterator(iterator)
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


class GroupAsyncIterator(_GroupAsyncIteratorMixin[T], AsyncIterator[List[T]]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        size: Optional[int],
        interval: Optional[datetime.timedelta],
    ) -> None:
        super().__init__(iterator, size, interval)
        self._current_group: List[T] = []

    async def __anext__(self) -> List[T]:
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
                self._current_group.append(await self.iterator.__anext__())
        except Exception as e:
            if not self._current_group:
                raise
            self._to_be_raised = e

        group, self._current_group = self._current_group, []
        self._remember_group_time()
        return group


class AGroupbyAsyncIterator(
    _GroupAsyncIteratorMixin[T], AsyncIterator[Tuple[U, List[T]]]
):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        key: Callable[[T], Coroutine[Any, Any, U]],
        size: Optional[int],
        interval: Optional[datetime.timedelta],
    ) -> None:
        super().__init__(iterator, size, interval)
        self.key = awrap_error(key, StopAsyncIteration)
        self._is_exhausted = False
        self._groups_by: DefaultDict[U, List[T]] = defaultdict(list)

    async def _group_next_elem(self) -> None:
        elem = await self.iterator.__anext__()
        self._groups_by[await self.key(elem)].append(elem)

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

    async def __anext__(self) -> Tuple[U, List[T]]:
        self._init_last_group_time()
        if self._is_exhausted:
            if self._groups_by:
                return self._pop_first_group()
            raise StopAsyncIteration

        if self._to_be_raised:
            if self._groups_by:
                self._remember_group_time()
                return self._pop_first_group()
            try:
                raise self._to_be_raised
            finally:
                self._to_be_raised = None

        try:
            await self._group_next_elem()

            full_group: Optional[Tuple[U, List[T]]] = self._pop_full_group()
            while not full_group and not self._interval_seconds_have_elapsed():
                await self._group_next_elem()
                full_group = self._pop_full_group()

            self._remember_group_time()
            return full_group or self._pop_largest_group()

        except StopAsyncIteration:
            self._is_exhausted = True
            return await self.__anext__()

        except Exception as e:
            self._to_be_raised = e
            return await self.__anext__()


class CountSkipAsyncIterator(AsyncIterator[T]):
    def __init__(self, iterator: AsyncIterator[T], count: int) -> None:
        validate_aiterator(iterator)
        validate_count(count)
        self.iterator = iterator
        self.count = count
        self._n_skipped = 0
        self._done_skipping = False

    async def __anext__(self) -> T:
        if not self._done_skipping:
            while self._n_skipped < self.count:
                await self.iterator.__anext__()
                # do not count exceptions as skipped elements
                self._n_skipped += 1
            self._done_skipping = True
        return await self.iterator.__anext__()


class PredicateASkipAsyncIterator(AsyncIterator[T]):
    def __init__(
        self, iterator: AsyncIterator[T], until: Callable[[T], Coroutine[Any, Any, Any]]
    ) -> None:
        validate_aiterator(iterator)
        self.iterator = iterator
        self.until = awrap_error(until, StopAsyncIteration)
        self._done_skipping = False

    async def __anext__(self) -> T:
        elem = await self.iterator.__anext__()
        if not self._done_skipping:
            while not await self.until(elem):
                elem = await self.iterator.__anext__()
            self._done_skipping = True
        return elem


class CountAndPredicateASkipAsyncIterator(AsyncIterator[T]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        count: int,
        until: Callable[[T], Coroutine[Any, Any, Any]],
    ) -> None:
        validate_aiterator(iterator)
        validate_count(count)
        self.iterator = iterator
        self.count = count
        self.until = awrap_error(until, StopAsyncIteration)
        self._n_skipped = 0
        self._done_skipping = False

    async def __anext__(self) -> T:
        elem = await self.iterator.__anext__()
        if not self._done_skipping:
            while self._n_skipped < self.count and not await self.until(elem):
                elem = await self.iterator.__anext__()
                # do not count exceptions as skipped elements
                self._n_skipped += 1
            self._done_skipping = True
        return elem


class CountTruncateAsyncIterator(AsyncIterator[T]):
    def __init__(self, iterator: AsyncIterator[T], count: int) -> None:
        validate_aiterator(iterator)
        validate_count(count)
        self.iterator = iterator
        self.count = count
        self._current_count = 0

    async def __anext__(self) -> T:
        if self._current_count == self.count:
            raise StopAsyncIteration()
        elem = await self.iterator.__anext__()
        self._current_count += 1
        return elem


class PredicateATruncateAsyncIterator(AsyncIterator[T]):
    def __init__(
        self, iterator: AsyncIterator[T], when: Callable[[T], Coroutine[Any, Any, Any]]
    ) -> None:
        validate_aiterator(iterator)
        self.iterator = iterator
        self.when = awrap_error(when, StopAsyncIteration)
        self._satisfied = False

    async def __anext__(self) -> T:
        if self._satisfied:
            raise StopAsyncIteration()
        elem = await self.iterator.__anext__()
        if await self.when(elem):
            self._satisfied = True
            raise StopAsyncIteration()
        return elem


class AMapAsyncIterator(AsyncIterator[U]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        transformation: Callable[[T], Coroutine[Any, Any, U]],
    ) -> None:
        validate_aiterator(iterator)

        self.iterator = iterator
        self.transformation = awrap_error(transformation, StopAsyncIteration)

    async def __anext__(self) -> U:
        return await self.transformation(await self.iterator.__anext__())


class AFilterAsyncIterator(AsyncIterator[T]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        when: Callable[[T], Coroutine[Any, Any, Any]],
    ) -> None:
        validate_aiterator(iterator)

        self.iterator = iterator
        self.when = awrap_error(when, StopAsyncIteration)

    async def __anext__(self) -> T:
        while True:
            elem = await self.iterator.__anext__()
            if await self.when(elem):
                return elem


class ObserveAsyncIterator(AsyncIterator[T]):
    def __init__(self, iterator: AsyncIterator[T], what: str, base: int = 2) -> None:
        validate_aiterator(iterator)
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

    async def __anext__(self) -> T:
        try:
            elem = await self.iterator.__anext__()
            self._n_nexts += 1
            self._n_yields += 1
            return elem
        except StopAsyncIteration:
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


class YieldsPerPeriodThrottleAsyncIterator(AsyncIterator[T]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        max_yields: int,
        period: datetime.timedelta,
    ) -> None:
        validate_aiterator(iterator)
        self.iterator = iterator
        self.max_yields = max_yields
        self._period_seconds = period.total_seconds()

        self._period_index: int = -1
        self._yields_in_period = 0

        self._offset: Optional[float] = None

    async def safe_next(self) -> Tuple[Optional[T], Optional[Exception]]:
        try:
            return await self.iterator.__anext__(), None
        except StopAsyncIteration:
            raise
        except Exception as e:
            return None, e

    async def __anext__(self) -> T:
        elem, caught_error = await self.safe_next()

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
            await asyncio.sleep(
                (ceil(num_periods) - num_periods) * self._period_seconds
            )
        self._yields_in_period += 1

        if caught_error:
            raise caught_error
        return cast(T, elem)


class _RaisingAsyncIterator(AsyncIterator[T]):
    class ExceptionContainer(NamedTuple):
        exception: Exception

    def __init__(
        self,
        iterator: AsyncIterator[Union[T, ExceptionContainer]],
    ) -> None:
        self.iterator = iterator

    async def __anext__(self) -> T:
        elem = await self.iterator.__anext__()
        if isinstance(elem, self.ExceptionContainer):
            raise elem.exception
        return elem


class _ConcurrentMapAsyncIterableMixin(
    Generic[T, U],
    ABC,
    AsyncIterable[Union[U, _RaisingAsyncIterator.ExceptionContainer]],
):
    """
    Template Method Pattern:
    This abstract class's `__iter__` is a skeleton for a queue-based concurrent mapping algorithm
    that relies on abstract helper methods (`_context_manager`, `_create_future`, `_future_result_collection`)
    that must be implemented by concrete subclasses.
    """

    def __init__(
        self,
        iterator: AsyncIterator[T],
        buffersize: int,
        ordered: bool,
    ) -> None:
        validate_aiterator(iterator)
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
    ) -> "Future[Union[U, _RaisingAsyncIterator.ExceptionContainer]]": ...

    # factory method
    @abstractmethod
    def _future_result_collection(
        self,
    ) -> FutureResultCollection[Union[U, _RaisingAsyncIterator.ExceptionContainer]]: ...

    async def __aiter__(
        self,
    ) -> AsyncIterator[Union[U, _RaisingAsyncIterator.ExceptionContainer]]:
        with self._context_manager():
            future_results = self._future_result_collection()

            # queue tasks up to buffersize
            with suppress(StopAsyncIteration):
                while len(future_results) < self.buffersize:
                    future_results.add_future(
                        self._launch_task(await self.iterator.__anext__())
                    )

            # wait, queue, yield
            while future_results:
                result = await future_results.__anext__()
                with suppress(StopAsyncIteration):
                    future_results.add_future(
                        self._launch_task(await self.iterator.__anext__())
                    )
                yield result


class _ConcurrentMapAsyncIterable(_ConcurrentMapAsyncIterableMixin[T, U]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        transformation: Callable[[T], U],
        concurrency: int,
        buffersize: int,
        ordered: bool,
        via: "Literal['thread', 'process']",
    ) -> None:
        super().__init__(iterator, buffersize, ordered)
        validate_concurrency(concurrency)
        self.transformation = wrap_error(transformation, StopAsyncIteration)
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
    ) -> Union[U, _RaisingAsyncIterator.ExceptionContainer]:
        try:
            return transformation(elem)
        except Exception as e:
            return _RaisingAsyncIterator.ExceptionContainer(e)

    def _launch_task(
        self, elem: T
    ) -> "Future[Union[U, _RaisingAsyncIterator.ExceptionContainer]]":
        return self.executor.submit(
            self._safe_transformation, self.transformation, elem
        )

    def _future_result_collection(
        self,
    ) -> FutureResultCollection[Union[U, _RaisingAsyncIterator.ExceptionContainer]]:
        if self.ordered:
            return FIFOOSFutureResultCollection()
        return FDFOOSFutureResultCollection(
            multiprocessing.Queue if self.via == "process" else queue.Queue
        )


class ConcurrentMapAsyncIterator(_RaisingAsyncIterator[U]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        transformation: Callable[[T], U],
        concurrency: int,
        buffersize: int,
        ordered: bool,
        via: "Literal['thread', 'process']",
    ) -> None:
        super().__init__(
            _ConcurrentMapAsyncIterable(
                iterator,
                transformation,
                concurrency,
                buffersize,
                ordered,
                via,
            ).__aiter__()
        )


class _ConcurrentAMapAsyncIterable(
    _ConcurrentMapAsyncIterableMixin[T, U], GetEventLoopMixin
):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        transformation: Callable[[T], Coroutine[Any, Any, U]],
        buffersize: int,
        ordered: bool,
    ) -> None:
        super().__init__(iterator, buffersize, ordered)
        self.transformation = awrap_error(transformation, StopAsyncIteration)

    async def _safe_transformation(
        self, elem: T
    ) -> Union[U, _RaisingAsyncIterator.ExceptionContainer]:
        try:
            coroutine = self.transformation(elem)
            if not isinstance(coroutine, Coroutine):
                raise TypeError(
                    f"`transformation` must be an async function i.e. a function returning a Coroutine but it returned a {type(coroutine)}",
                )
            return await coroutine
        except Exception as e:
            return _RaisingAsyncIterator.ExceptionContainer(e)

    def _launch_task(
        self, elem: T
    ) -> "Future[Union[U, _RaisingAsyncIterator.ExceptionContainer]]":
        return cast(
            "Future[Union[U, _RaisingAsyncIterator.ExceptionContainer]]",
            self.get_event_loop().create_task(self._safe_transformation(elem)),
        )

    def _future_result_collection(
        self,
    ) -> FutureResultCollection[Union[U, _RaisingAsyncIterator.ExceptionContainer]]:
        if self.ordered:
            return FIFOAsyncFutureResultCollection(self.get_event_loop())
        else:
            return FDFOAsyncFutureResultCollection(self.get_event_loop())


class ConcurrentAMapAsyncIterator(_RaisingAsyncIterator[U]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        transformation: Callable[[T], Coroutine[Any, Any, U]],
        buffersize: int,
        ordered: bool,
    ) -> None:
        super().__init__(
            _ConcurrentAMapAsyncIterable(
                iterator,
                transformation,
                buffersize,
                ordered,
            ).__aiter__()
        )


class _ConcurrentFlattenAsyncIterable(
    AsyncIterable[Union[T, _RaisingAsyncIterator.ExceptionContainer]]
):
    def __init__(
        self,
        iterables_iterator: AsyncIterator[Iterable[T]],
        concurrency: int,
        buffersize: int,
    ) -> None:
        validate_aiterator(iterables_iterator)
        validate_concurrency(concurrency)
        self.iterables_iterator = iterables_iterator
        self.concurrency = concurrency
        self.buffersize = buffersize

    async def __aiter__(
        self,
    ) -> AsyncIterator[Union[T, _RaisingAsyncIterator.ExceptionContainer]]:
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            iterator_and_future_pairs: Deque[Tuple[Iterator[T], Future]] = deque()
            element_to_yield: Deque[
                Union[T, _RaisingAsyncIterator.ExceptionContainer]
            ] = deque(maxlen=1)
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
                        element_to_yield.append(
                            _RaisingAsyncIterator.ExceptionContainer(e)
                        )
                        iterator_to_queue = iterator

                # queue tasks up to buffersize
                while len(iterator_and_future_pairs) < self.buffersize:
                    if not iterator_to_queue:
                        try:
                            iterable = await self.iterables_iterator.__anext__()
                        except StopAsyncIteration:
                            break
                        try:
                            iterator_to_queue = iter_wo_stopasynciteration(iterable)
                        except Exception as e:
                            yield _RaisingAsyncIterator.ExceptionContainer(e)
                            continue
                    future = executor.submit(next, iterator_to_queue)
                    iterator_and_future_pairs.append((iterator_to_queue, future))
                    iterator_to_queue = None
                if element_to_yield:
                    yield element_to_yield.pop()
                if not iterator_and_future_pairs:
                    break


class ConcurrentFlattenAsyncIterator(_RaisingAsyncIterator[T]):
    def __init__(
        self,
        iterables_iterator: AsyncIterator[Iterable[T]],
        concurrency: int,
        buffersize: int,
    ) -> None:
        super().__init__(
            _ConcurrentFlattenAsyncIterable(
                iterables_iterator,
                concurrency,
                buffersize,
            ).__aiter__()
        )


class _ConcurrentAFlattenAsyncIterable(
    AsyncIterable[Union[T, _RaisingAsyncIterator.ExceptionContainer]], GetEventLoopMixin
):
    def __init__(
        self,
        iterables_iterator: AsyncIterator[AsyncIterable[T]],
        concurrency: int,
        buffersize: int,
    ) -> None:
        validate_aiterator(iterables_iterator)
        validate_concurrency(concurrency)
        self.iterables_iterator = iterables_iterator
        self.concurrency = concurrency
        self.buffersize = buffersize

    async def __aiter__(
        self,
    ) -> AsyncIterator[Union[T, _RaisingAsyncIterator.ExceptionContainer]]:
        iterator_and_future_pairs: Deque[Tuple[AsyncIterator[T], Awaitable[T]]] = (
            deque()
        )
        element_to_yield: Deque[Union[T, _RaisingAsyncIterator.ExceptionContainer]] = (
            deque(maxlen=1)
        )
        iterator_to_queue: Optional[AsyncIterator[T]] = None
        # wait, queue, yield (FIFO)
        while True:
            if iterator_and_future_pairs:
                iterator, future = iterator_and_future_pairs.popleft()
                try:
                    element_to_yield.append(await future)
                    iterator_to_queue = iterator
                except StopAsyncIteration:
                    pass
                except Exception as e:
                    element_to_yield.append(_RaisingAsyncIterator.ExceptionContainer(e))
                    iterator_to_queue = iterator

            # queue tasks up to buffersize
            while len(iterator_and_future_pairs) < self.buffersize:
                if not iterator_to_queue:
                    try:
                        iterable = await self.iterables_iterator.__anext__()
                    except StopAsyncIteration:
                        break
                    try:
                        iterator_to_queue = aiter_wo_stopasynciteration(iterable)
                    except Exception as e:
                        yield _RaisingAsyncIterator.ExceptionContainer(e)
                        continue
                future = self.get_event_loop().create_task(
                    awaitable_to_coroutine(
                        cast(AsyncIterator, iterator_to_queue).__anext__()
                    )
                )
                iterator_and_future_pairs.append(
                    (cast(AsyncIterator, iterator_to_queue), future)
                )
                iterator_to_queue = None
            if element_to_yield:
                yield element_to_yield.pop()
            if not iterator_and_future_pairs:
                break


class ConcurrentAFlattenAsyncIterator(_RaisingAsyncIterator[T]):
    def __init__(
        self,
        iterables_iterator: AsyncIterator[AsyncIterable[T]],
        concurrency: int,
        buffersize: int,
    ) -> None:
        super().__init__(
            _ConcurrentAFlattenAsyncIterable(
                iterables_iterator,
                concurrency,
                buffersize,
            ).__aiter__()
        )
