import datetime
import queue
from threading import Semaphore, Thread
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque

from concurrent.futures import Executor, Future, ThreadPoolExecutor
from contextlib import suppress
from typing import (
    Any,
    Callable,
    ContextManager,
    Deque,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)
import weakref

from streamable._tools._context import noop_context_manager
from streamable._tools._observation import Observation
from streamable._tools._sentinel import STOP_ITERATION
from streamable._tools._validation import validate_sync_flatten_iterable

from streamable._tools._error import ExceptionContainer, RaisingIterator

from streamable._tools._future import (
    FDFOFutureResults,
    FIFOFutureResults,
    FutureResult,
    FutureResults,
)

T = TypeVar("T")
U = TypeVar("U")
Exc = TypeVar("Exc", bound=Exception)


##########
# buffer #
##########


class _BufferIterable(Iterable[Union[T, ExceptionContainer]]):
    __slots__ = ("iterator", "_buffer", "_slots", "_stopped")

    def __init__(
        self,
        iterator: Iterator[T],
        up_to: int,
    ) -> None:
        self.iterator = iterator
        self._buffer: "queue.Queue[Union[T, ExceptionContainer]]" = queue.Queue()
        self._slots = Semaphore(up_to)
        self._stopped = False

    def _buffer_upstream(self) -> None:
        elem: Union[T, ExceptionContainer]
        self._slots.acquire()
        while not self._stopped:
            try:
                elem = self.iterator.__next__()
            except StopIteration:
                elem = STOP_ITERATION
                self._stopped = True
            except Exception as e:
                elem = ExceptionContainer(e)
            self._buffer.put_nowait(elem)
            self._slots.acquire()

    def __iter__(self) -> Iterator[Union[T, ExceptionContainer]]:
        thread = Thread(target=self._buffer_upstream, daemon=True)
        to_yield: Deque[Union[T, ExceptionContainer]] = deque(maxlen=1)
        try:
            thread.start()
            while True:
                to_yield.append(self._buffer.get())
                if to_yield[-1] is STOP_ITERATION:
                    break
                self._slots.release()
                yield to_yield.pop()
        finally:
            self._stopped = True
            self._slots.release()
            thread.join()


class BufferIterator(RaisingIterator[T]):
    __slots__ = ()

    def __init__(
        self,
        iterator: Iterator[T],
        up_to: int,
    ) -> None:
        super().__init__(_BufferIterable(iterator, up_to).__iter__())


#########
# catch #
#########


class CatchIterator(Iterator[Union[T, U]]):
    __slots__ = ("iterator", "errors", "where", "replace", "do", "stop", "_stopped")

    def __init__(
        self,
        iterator: Iterator[T],
        errors: Union[Type[Exc], Tuple[Type[Exc], ...]],
        where: Optional[Callable[[Exc], Any]],
        replace: Optional[Callable[[Exc], U]],
        do: Optional[Callable[[Exc], Any]],
        stop: bool,
    ) -> None:
        self.iterator = iterator
        self.errors = errors
        self.where = where
        self.replace = replace
        self.do = do
        self.stop = stop
        self._stopped = False

    def __next__(self) -> Union[T, U]:
        while True:
            if self._stopped:
                raise StopIteration
            try:
                return self.iterator.__next__()
            except StopIteration:
                raise
            except self.errors as e:
                if not self.where or self.where(e):
                    if self.stop:
                        self._stopped = True
                    if self.do:
                        self.do(e)
                    if self.replace:
                        return self.replace(e)
                    continue
                raise


###########
# flatten #
###########


class FlattenIterator(Iterator[T]):
    __slots__ = ("iterator", "_current_iterator_elem")

    def __init__(
        self,
        iterator: Iterator[Iterable[T]],
    ) -> None:
        self.iterator = iterator
        self._current_iterator_elem: Iterator[T] = ().__iter__()

    def __next__(self) -> T:
        while True:
            try:
                return self._current_iterator_elem.__next__()
            except StopIteration:
                iterable = self.iterator.__next__()
                validate_sync_flatten_iterable(iterable)
                self._current_iterator_elem = iterable.__iter__()


#########
# group #
#########


class GroupIterator(Iterator[List[T]]):
    __slots__ = ("iterator", "up_to", "_group", "_to_raise")

    def __init__(
        self,
        iterator: Iterator[T],
        up_to: Optional[int],
    ) -> None:
        self.iterator = iterator
        self.up_to = up_to or cast(int, float("inf"))
        self._group: List[T] = []
        self._to_raise: Optional[Exception] = None

    def __next__(self) -> List[T]:
        if self._to_raise:
            try:
                raise self._to_raise
            finally:
                self._to_raise = None
        while len(self._group) < self.up_to:
            try:
                self._group.append(self.iterator.__next__())
            except Exception as e:
                if self._group:
                    self._to_raise = e
                    break
                raise
        try:
            return self._group
        finally:
            self._group = []


class GroupByIterator(Iterator[Iterable[Tuple[U, List[T]]]]):
    __slots__ = ("iterator", "up_to", "by", "_groups", "_to_raise")

    def __init__(
        self,
        iterator: Iterator[T],
        up_to: Optional[int],
        by: Callable[[T], U],
    ) -> None:
        self.iterator = iterator
        self.up_to = up_to or cast(int, float("inf"))
        self.by = by
        self._groups: Dict[U, List[T]] = defaultdict(list)
        self._to_raise: Optional[Exception] = None

    def __next__(self) -> Iterable[Tuple[U, List[T]]]:
        if self._to_raise:
            try:
                raise self._to_raise
            finally:
                self._to_raise = None
        while True:
            try:
                elem = self.iterator.__next__()
                key = self.by(elem)
                group = self._groups[key]
                group.append(elem)
                if len(group) == self.up_to:
                    del self._groups[key]
                    return ((key, group),)
            except Exception as e:
                if self._groups:
                    self._to_raise = e
                    try:
                        return self._groups.items()
                    finally:
                        self._groups = defaultdict(list)
                raise


class _GroupByWithinIterable(Iterable[Union[ExceptionContainer, Tuple[U, List[T]]]]):
    __slots__ = (
        "iterator",
        "up_to",
        "by",
        "_within_seconds",
        "_groups",
        "_next_elem",
        "_let_pull_next",
        "_stopped",
    )

    def __init__(
        self,
        iterator: Iterator[T],
        up_to: Optional[int],
        by: Callable[[T], U],
        within: datetime.timedelta,
    ) -> None:
        self.iterator = iterator
        self.up_to = up_to or cast(int, float("inf"))
        self.by = by
        self._groups: Dict[U, Tuple[float, List[T]]] = self._default_groups()
        self._within_seconds = within.total_seconds()
        self._next_elem: queue.Queue[Union[T, ExceptionContainer]] = queue.Queue()
        self._let_pull_next: Semaphore = Semaphore(0)
        self._stopped = False

    @staticmethod
    def _default_groups() -> Dict[U, Tuple[float, List[T]]]:
        return defaultdict(lambda: (time.perf_counter(), []))

    def _oldest_group(self) -> Tuple[U, List[T]]:
        oldest_key = next(iter(self._groups.keys()))
        return (oldest_key, self._groups.pop(oldest_key)[1])

    def _timeout(self) -> Optional[float]:
        if self._groups:
            oldest_group_time = next(iter(self._groups.values()))[0]
            timeout = oldest_group_time + self._within_seconds - time.perf_counter()
            return max(0, timeout)
        return None

    def _pull_upstream(self) -> None:
        elem: Union[T, ExceptionContainer]
        self._let_pull_next.acquire()
        while not self._stopped:
            try:
                elem = self.iterator.__next__()
            except StopIteration:
                elem = STOP_ITERATION
                self._stopped = True
            except Exception as e:
                elem = ExceptionContainer(e)
            try:
                self._next_elem.put_nowait(elem)
            finally:
                del elem
            self._let_pull_next.acquire()

    def _get_next_elem(self) -> T:
        elem = self._next_elem.get(timeout=self._timeout())
        if elem is STOP_ITERATION:
            raise StopIteration
        if isinstance(elem, ExceptionContainer):
            try:
                raise elem.exception
            finally:
                del elem
        return elem

    def __iter__(self) -> Iterator[Union[ExceptionContainer, Tuple[U, List[T]]]]:
        thread = Thread(target=self._pull_upstream, daemon=True)
        try:
            thread.start()
            while True:
                self._let_pull_next.release()
                try:
                    while True:
                        try:
                            elem = self._get_next_elem()
                            break
                        except queue.Empty:
                            yield self._oldest_group()
                    key = self.by(elem)
                    _, group = self._groups[key]
                    group.append(elem)
                    if len(group) == self.up_to:
                        del self._groups[key]
                        yield (key, group)
                    continue
                except Exception as e:
                    # upstream stopped iteration, or raised an error, or `by` did
                    while self._groups:
                        yield self._oldest_group()
                    if isinstance(e, StopIteration):
                        return
                    error = [ExceptionContainer(e)]
                # yield outside the except block so the frame's exception state is cleared
                yield error.pop()
        finally:
            self._stopped = True
            self._let_pull_next.release()
            thread.join()


class GroupByWithinIterator(RaisingIterator[Tuple[U, List[T]]]):
    def __init__(
        self,
        iterator: Iterator[T],
        up_to: Optional[int],
        by: Callable[[T], U],
        within: datetime.timedelta,
    ) -> None:
        super().__init__(iter(_GroupByWithinIterable(iterator, up_to, by, within)))


########
# skip #
########


class CountSkipIterator(Iterator[T]):
    __slots__ = ("iterator", "_remaining_to_skip")

    def __init__(self, iterator: Iterator[T], count: int) -> None:
        self.iterator = iterator
        self._remaining_to_skip = count

    def __next__(self) -> T:
        while self._remaining_to_skip > 0:
            self.iterator.__next__()
            # do not count exceptions as skipped elements
            self._remaining_to_skip -= 1
        return self.iterator.__next__()


class PredicateSkipIterator(Iterator[T]):
    __slots__ = ("iterator", "until", "_satisfied")

    def __init__(self, iterator: Iterator[T], until: Callable[[T], Any]) -> None:
        self.iterator = iterator
        self.until = until
        self._satisfied = False

    def __next__(self) -> T:
        elem = self.iterator.__next__()
        if not self._satisfied:
            while not self.until(elem):
                elem = self.iterator.__next__()
            self._satisfied = True
        return elem


############
# take #
############


class CountTakeIterator(Iterator[T]):
    __slots__ = ("iterator", "_remaining_to_take")

    def __init__(self, iterator: Iterator[T], count: int) -> None:
        self.iterator = iterator
        self._remaining_to_take = count

    def __next__(self) -> T:
        if self._remaining_to_take <= 0:
            raise StopIteration
        elem = self.iterator.__next__()
        self._remaining_to_take -= 1
        return elem


class PredicateTakeIterator(Iterator[T]):
    __slots__ = ("iterator", "until", "_satisfied")

    def __init__(self, iterator: Iterator[T], until: Callable[[T], Any]) -> None:
        self.iterator = iterator
        self.until = until
        self._satisfied = False

    def __next__(self) -> T:
        if self._satisfied:
            raise StopIteration
        elem = self.iterator.__next__()
        if self.until(elem):
            self._satisfied = True
            raise StopIteration
        return elem


###########
# observe #
###########


class _BaseObserveIterator(Iterator[T]):
    __slots__ = (
        "iterator",
        "subject",
        "do",
        "_elements",
        "_errors",
        "_emissions_observed",
        "_elements_observed",
        "_errors_observed",
        "_active",
        "_start_point",
    )

    def __init__(
        self,
        iterator: Iterator[T],
        subject: str,
        do: Callable[[Observation], Any],
    ) -> None:
        self.iterator = iterator
        self.subject = subject
        self.do = do
        self._elements = 0
        self._errors = 0
        self._emissions_observed = 0
        self._elements_observed = 0
        self._errors_observed = 0
        self._active = False
        self._start_point: datetime.datetime

    @property
    def _emissions(self) -> int:
        return self._elements + self._errors

    def _observation(self) -> Observation:
        return Observation(
            subject=self.subject,
            elapsed=self._time_point() - self._start_point,
            errors=self._errors,
            elements=self._elements,
        )

    @staticmethod
    def _time_point() -> datetime.datetime:
        return datetime.datetime.fromtimestamp(time.perf_counter())

    def _activate(self) -> None:
        self._start_point = self._time_point()
        self._active = True

    def _observe(self) -> None:
        self._emissions_observed = self._emissions
        with suppress(Exception):
            self.do(self._observation())

    @abstractmethod
    def _threshold(self, observed: int) -> int: ...

    def __next__(self) -> T:
        if not self._active:
            self._activate()
        try:
            elem = self.iterator.__next__()
            self._elements += 1
            if self._elements >= self._threshold(self._elements_observed):
                self._observe()
                self._elements_observed = self._elements
            return elem
        except StopIteration:
            if not self._emissions or self._emissions > self._emissions_observed:
                self._observe()
            self._active = False
            raise
        except Exception:
            self._errors += 1
            if self._errors >= self._threshold(self._errors_observed):
                self._observe()
                self._errors_observed = self._errors
            raise


class PowerObserveIterator(_BaseObserveIterator[T]):
    __slots__ = ("base",)

    def __init__(
        self,
        iterator: Iterator[T],
        subject: str,
        do: Callable[[Observation], Any],
        base: int = 2,
    ) -> None:
        super().__init__(iterator, subject, do)
        self.base = base

    def _threshold(self, observed: int) -> int:
        return self.base * observed


class EveryIntObserveIterator(_BaseObserveIterator[T]):
    __slots__ = ("every",)

    def __init__(
        self,
        iterator: Iterator[T],
        subject: str,
        every: int,
        do: Callable[[Observation], Any],
    ) -> None:
        super().__init__(iterator, subject, do)
        self.every = every

    def _threshold(self, observed: int) -> int:
        if not observed:
            return 0
        if observed == 1:
            return self.every
        return observed + self.every


class EveryIntervalObserveIterator(_BaseObserveIterator[T]):
    __slots__ = ("__weakref__", "every")

    def __init__(
        self,
        iterator: Iterator[T],
        subject: str,
        every: datetime.timedelta,
        do: Callable[[Observation], Any],
    ) -> None:
        super().__init__(iterator, subject, do)
        self.every = every

    @staticmethod
    def _observer(
        weak_self: "weakref.ReferenceType[EveryIntervalObserveIterator[T]]",
        every_seconds: float,
    ) -> None:
        self = weak_self()
        while self and self._active:
            self._observe()
            self = None
            time.sleep(every_seconds)
            self = weak_self()

    def _activate(self) -> None:
        super()._activate()
        Thread(
            target=self._observer,
            args=(weakref.ref(self), self.every.total_seconds()),
            daemon=True,
        ).start()

    def _threshold(self, observed: int) -> int:
        return cast(int, float("inf"))


############
# throttle #
############


class ThrottleIterator(Iterator[T]):
    __slots__ = ("iterator", "up_to", "_window_seconds", "_emission_timestamps")

    def __init__(
        self,
        iterator: Iterator[T],
        up_to: int,
        per: datetime.timedelta,
    ) -> None:
        self.iterator = iterator
        self.up_to = up_to
        self._window_seconds = per.total_seconds()
        self._emission_timestamps: Deque[float] = deque()

    def __next__(self) -> T:
        elem: Optional[T] = None
        error: Optional[Exception] = None
        try:
            elem = self.iterator.__next__()
        except StopIteration:
            raise
        except Exception as e:
            error = e

        # did we reach `up_to` emissions?
        if len(self._emission_timestamps) >= self.up_to:
            # sleep until the oldest emission leaves the window
            oldest_leaves_window_at = (
                self._emission_timestamps[0] + self._window_seconds
            )
            time.sleep(max(0, oldest_leaves_window_at - time.perf_counter()))
            # remove the oldest emission
            self._emission_timestamps.popleft()

        # register the new emission
        self._emission_timestamps.append(time.perf_counter())
        if error:
            try:
                raise error
            finally:
                error = None
        return cast(T, elem)


##################
# concurrent map #
##################


class _ConcurrentMapIterable(
    Generic[T, U], ABC, Iterable[Union[U, ExceptionContainer]]
):
    __slots__ = (
        "iterator",
        "into",
        "concurrency",
        "_executor",
        "_context_manager",
        "_future_results",
    )

    def __init__(
        self,
        iterator: Iterator[T],
        into: Callable[[T], U],
        concurrency: Union[int, Executor],
        as_completed: bool,
    ) -> None:
        self.iterator = iterator
        self.into = ExceptionContainer.wrap(into)
        self._future_results: FutureResults[Union[U, ExceptionContainer]] = (
            FDFOFutureResults() if as_completed else FIFOFutureResults()
        )
        self._context_manager: ContextManager
        if isinstance(concurrency, int):
            self._executor: Executor = ThreadPoolExecutor(max_workers=concurrency)
            self.concurrency = concurrency
            self._context_manager = self._executor
        else:
            self._executor = concurrency
            self.concurrency = getattr(self._executor, "_max_workers")
            self._context_manager = noop_context_manager()

    def _launch_task(self, elem: T) -> "Future[Union[U, ExceptionContainer]]":
        return self._executor.submit(self.into, elem)

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
        with self._context_manager:
            # queue tasks up to buffersize
            while len(self._future_results) < self.concurrency:
                future = self._next_future()
                if not future:
                    # no more tasks to queue
                    break
                self._future_results.add(future)
                del future

            # queue, wait, yield
            while self._future_results:
                future = self._next_future()
                if future:
                    self._future_results.add(future)
                    del future
                yield self._future_results.__next__()


class ConcurrentMapIterator(RaisingIterator[U]):
    __slots__ = ()

    def __init__(
        self,
        iterator: Iterator[T],
        into: Callable[[T], U],
        concurrency: Union[int, Executor],
        as_completed: bool,
    ) -> None:
        super().__init__(
            _ConcurrentMapIterable(
                iterator,
                into,
                concurrency,
                as_completed,
            ).__iter__()
        )


######################
# concurrent flatten #
######################


class _ConcurrentFlattenIterable(Iterable[Union[T, ExceptionContainer]]):
    __slots__ = (
        "iterables_iterator",
        "concurrency",
        "_next",
    )

    def __init__(
        self,
        iterables_iterator: Iterator[Iterable[T]],
        concurrency: int,
    ) -> None:
        self.iterables_iterator = iterables_iterator
        self.concurrency = concurrency
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
                        del elem
                        del future
                        iterator_to_queue = iterator

                # queue tasks up to buffersize
                while len(iterator_and_future_pairs) < self.concurrency:
                    if not iterator_to_queue:
                        try:
                            try:
                                iterable = self.iterables_iterator.__next__()
                            except StopIteration:
                                break
                            validate_sync_flatten_iterable(iterable)
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


class ConcurrentFlattenIterator(RaisingIterator[T]):
    __slots__ = ()

    def __init__(
        self,
        iterables_iterator: Iterator[Iterable[T]],
        concurrency: int,
    ) -> None:
        super().__init__(
            _ConcurrentFlattenIterable(
                iterables_iterator,
                concurrency,
            ).__iter__()
        )
