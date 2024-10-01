import time
from abc import ABC, abstractmethod
from asyncio import AbstractEventLoop, get_event_loop
from collections import defaultdict, deque
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from contextlib import contextmanager, suppress
from datetime import datetime
from math import ceil
from multiprocessing import get_logger
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
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from streamable.util.functiontools import reraise_as

T = TypeVar("T")
U = TypeVar("U")

from streamable.util.constants import NO_REPLACEMENT
from streamable.util.exceptions import NoopStopIteration
from streamable.util.futuretools import (
    FDFOAsyncFutureResultCollection,
    FDFOThreadFutureResultCollection,
    FIFOAsyncFutureResultCollection,
    FIFOThreadFutureResultCollection,
    FutureResultCollection,
)


class CatchingIterator(Iterator[T]):
    def __init__(
        self,
        iterator: Iterator[T],
        kind: Type[Exception],
        when: Callable[[Exception], Any],
        replacement: T,
        finally_raise: bool,
    ) -> None:
        self.iterator = iterator
        self.kind = kind
        self.when = when
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


class FlatteningIterator(Iterator[U]):
    def __init__(self, iterator: Iterator[Iterable[U]]) -> None:
        self.iterator = iterator
        self._current_iterator_elem: Iterator[U] = iter([])

    def __next__(self) -> U:
        while True:
            try:
                return next(self._current_iterator_elem)
            except StopIteration:
                iterable_elem = next(self.iterator)
                self._current_iterator_elem = reraise_as(
                    iter, StopIteration, NoopStopIteration
                )(iterable_elem)


class GroupingIterator(Iterator[List[T]]):
    def __init__(
        self,
        iterator: Iterator[T],
        size: int,
        interval_seconds: float,
    ) -> None:
        self.iterator = iterator
        self.size = size
        self.interval_seconds = interval_seconds
        self._to_be_raised: Optional[Exception] = None
        self._last_group_yielded_at: float = 0
        self._current_group: List[T] = []

    def _interval_seconds_have_elapsed(self) -> bool:
        return (time.time() - self._last_group_yielded_at) >= self.interval_seconds

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


class GroupingByIterator(GroupingIterator[T]):
    def __init__(
        self,
        iterator: Iterator[T],
        size: int,
        interval_seconds: float,
        by: Optional[Callable[[T], Any]],
    ) -> None:
        super().__init__(iterator, size, interval_seconds)
        self.by = by
        self._is_exhausted = False
        self._groups_by: DefaultDict[Any, List[T]] = defaultdict(list)

    def _group_key(self, elem: T) -> Any:
        if self.by:
            return self.by(elem)

    def _group_next_elem(self) -> None:
        elem = next(self.iterator)
        key = self._group_key(elem)
        self._groups_by[key].append(elem)

    def _interval_seconds_have_elapsed(self) -> bool:
        return (time.time() - self._last_group_yielded_at) >= self.interval_seconds

    def _pop_full_group(self) -> Optional[List[T]]:
        for key, group in self._groups_by.items():
            if len(group) >= self.size:
                return self._groups_by.pop(key)
        return None

    def _pop_first_group(self) -> List[T]:
        first_key = next(iter(self._groups_by), ...)
        return self._groups_by.pop(first_key)

    def _pop_largest_group(self) -> List[T]:
        largest_group_key: Any = next(iter(self._groups_by), ...)

        for key, group in self._groups_by.items():
            if len(group) > len(self._groups_by[largest_group_key]):
                largest_group_key = key

        return self._groups_by.pop(largest_group_key)

    def _return_group(self, group: List[T]) -> List[T]:
        self._last_group_yielded_at = time.time()
        return group

    def __next__(self) -> List[T]:
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

            full_group: Optional[List[T]] = self._pop_full_group()
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


class TruncatingOnCountIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], count: int) -> None:
        self.iterator = iterator
        self.max_count = count
        self.count = 0

    def __next__(self):
        if self.count == self.max_count:
            raise StopIteration()
        elem = next(self.iterator)
        self.count += 1
        return elem


class TruncatingOnPredicateIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], when: Callable[[T], Any]) -> None:
        self.iterator = iterator
        self.when = when
        self.satisfied = False

    def __next__(self):
        if self.satisfied:
            raise StopIteration()
        elem = next(self.iterator)
        if self.when(elem):
            self.satisfied = True
            return next(self)
        return elem


class ObservingIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], what: str) -> None:
        self.iterator = iterator
        self.what = what
        self._n_yields = 0
        self._n_errors = 0
        self._logged_n_calls = 0
        self._start_time = time.time()

    def _log(self) -> None:
        get_logger().info(
            "[%s %s] %s",
            f"duration={datetime.fromtimestamp(time.time()) - datetime.fromtimestamp(self._start_time)}",
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


class ThrottlingIntervalIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], interval_seconds: float) -> None:
        self.iterator = iterator
        self.interval_seconds = interval_seconds

    def __next__(self) -> T:
        start_time = time.time()
        elem = next(self.iterator)
        elapsed_time = time.time() - start_time
        if self.interval_seconds > elapsed_time:
            time.sleep(self.interval_seconds - elapsed_time)
        return elem


class ThrottlingPerSecondIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], per_second: int) -> None:
        self.iterator = iterator
        self.per_second = per_second

        self.second: int = -1
        self.yields_in_second = 0
        self.offset: Optional[float] = None

    def __next__(self) -> T:
        current_time = time.time()
        if not self.offset:
            self.offset = current_time
        current_time -= self.offset
        current_second = int(current_time)
        if self.second != current_second:
            self.second = current_second
            self.yields_in_second = 0

        if self.yields_in_second >= self.per_second:
            # sleep until next second
            time.sleep(ceil(current_time) - current_time)
            return next(self)

        self.yields_in_second += 1
        return next(self.iterator)


class RaisingIterator(Iterator[T]):
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


class ConcurrentMappingIterable(
    Generic[T, U], ABC, Iterable[Union[U, RaisingIterator.ExceptionContainer]]
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
        buffer_size: int,
        ordered: bool,
    ) -> None:
        self.iterator = iterator
        self.buffer_size = buffer_size
        self.ordered = ordered

    @abstractmethod
    def _context_manager(self) -> ContextManager: ...

    @abstractmethod
    def _create_future(
        self, elem: T
    ) -> "Future[Union[U, RaisingIterator.ExceptionContainer]]": ...

    # factory method
    @abstractmethod
    def _future_result_collection(
        self,
    ) -> FutureResultCollection[Union[U, RaisingIterator.ExceptionContainer]]: ...

    def __iter__(self) -> Iterator[Union[U, RaisingIterator.ExceptionContainer]]:
        with self._context_manager():
            future_results = self._future_result_collection()

            # queue tasks up to buffer_size
            with suppress(StopIteration):
                while len(future_results) < self.buffer_size:
                    future_results.add_future(self._create_future(next(self.iterator)))

            # wait, queue, yield
            while future_results:
                result = next(future_results)
                with suppress(StopIteration):
                    future_results.add_future(self._create_future(next(self.iterator)))
                yield result


class ThreadConcurrentMappingIterable(ConcurrentMappingIterable[T, U]):
    def __init__(
        self,
        iterator: Iterator[T],
        transformation: Callable[[T], U],
        concurrency: int,
        buffer_size: int,
        ordered: bool,
    ) -> None:
        super().__init__(iterator, buffer_size, ordered)
        self.transformation = transformation
        self.concurrency = concurrency
        self.executor: Executor

    def _context_manager(self) -> ContextManager:
        self.executor = ThreadPoolExecutor(max_workers=self.concurrency)
        return self.executor

    # picklable
    @staticmethod
    def _safe_transformation(
        transformation: Callable[[T], U], elem: T
    ) -> Union[U, RaisingIterator.ExceptionContainer]:
        try:
            return transformation(elem)
        except Exception as e:
            return RaisingIterator.ExceptionContainer(e)

    def _create_future(
        self, elem: T
    ) -> "Future[Union[U, RaisingIterator.ExceptionContainer]]":
        return self.executor.submit(
            self._safe_transformation, self.transformation, elem
        )

    def _future_result_collection(
        self,
    ) -> FutureResultCollection[Union[U, RaisingIterator.ExceptionContainer]]:
        if self.ordered:
            return FIFOThreadFutureResultCollection()
        else:
            return FDFOThreadFutureResultCollection()


class AsyncConcurrentMappingIterable(ConcurrentMappingIterable[T, U]):
    def __init__(
        self,
        iterator: Iterator[T],
        transformation: Callable[[T], Coroutine[Any, Any, U]],
        buffer_size: int,
        ordered: bool,
    ) -> None:
        super().__init__(iterator, buffer_size, ordered)
        self.transformation = transformation
        self._loop: AbstractEventLoop

    @contextmanager
    def _context_manager(self):
        self._loop = get_event_loop()
        yield

    async def _safe_transformation(
        self, elem: T
    ) -> Union[U, RaisingIterator.ExceptionContainer]:
        try:
            coroutine = self.transformation(elem)
            if not isinstance(coroutine, Coroutine):
                raise TypeError(
                    f"The function is expected to be an async function, i.e. it must be a function returning a Coroutine object, but returned a {type(coroutine)}."
                )
            return await coroutine
        except Exception as e:
            return RaisingIterator.ExceptionContainer(e)

    def _create_future(
        self, elem: T
    ) -> "Future[Union[U, RaisingIterator.ExceptionContainer]]":
        return cast(
            "Future[Union[U, RaisingIterator.ExceptionContainer]]",
            self._loop.create_task(self._safe_transformation(elem)),
        )

    def _future_result_collection(
        self,
    ) -> FutureResultCollection[Union[U, RaisingIterator.ExceptionContainer]]:
        if self.ordered:
            return FIFOAsyncFutureResultCollection(self._loop)
        else:
            return FDFOAsyncFutureResultCollection(self._loop)


class ConcurrentFlatteningIterable(
    Iterable[Union[T, RaisingIterator.ExceptionContainer]]
):
    def __init__(
        self,
        iterables_iterator: Iterator[Iterable[T]],
        concurrency: int,
        buffer_size: int,
    ) -> None:
        self.iterables_iterator = iterables_iterator
        self.concurrency = concurrency
        self.buffer_size = buffer_size

    def __iter__(self) -> Iterator[Union[T, RaisingIterator.ExceptionContainer]]:
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            iterator_and_future_pairs: Deque[Tuple[Iterator[T], Future]] = deque()
            element_to_yield: Deque[Union[T, RaisingIterator.ExceptionContainer]] = (
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
                        element_to_yield.append(RaisingIterator.ExceptionContainer(e))
                        iterator_to_queue.append(iterator)

                # queue tasks up to buffer_size
                while len(iterator_and_future_pairs) < self.buffer_size:
                    if iterator_to_queue:
                        iterator = iterator_to_queue.pop()
                    else:
                        try:
                            iterable = next(self.iterables_iterator)
                        except StopIteration:
                            break
                        try:
                            iterator = reraise_as(
                                iter, StopIteration, NoopStopIteration
                            )(iterable)
                        except Exception as e:
                            yield RaisingIterator.ExceptionContainer(e)
                            continue
                    future = executor.submit(
                        cast(Callable[[Iterable[T]], T], next), iterator
                    )
                    iterator_and_future_pairs.append((iterator, future))
                if element_to_yield:
                    yield element_to_yield.pop()
                if not iterator_and_future_pairs:
                    break
