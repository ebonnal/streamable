import datetime
import queue
from threading import Semaphore, Thread
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque

from concurrent.futures import Executor, Future, ThreadPoolExecutor
from contextlib import suppress
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ContextManager,
    DefaultDict,
    Deque,
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

from streamable._tools._contextmanager import noop_context_manager
from streamable._tools._sentinel import STOP_ITERATION
from streamable._tools._validation import validate_sync_flatten_iterable

if TYPE_CHECKING:
    from streamable._stream import stream
from streamable._tools._error import ExceptionContainer, RaisingIterator

from streamable._tools._future import (
    FDFOFutureResults,
    FIFOFutureResults,
    FutureResult,
    FutureResults,
)

with suppress(ImportError):
    pass

T = TypeVar("T")
U = TypeVar("U")


#########
# catch #
#########


class CatchIterator(Iterator[Union[T, U]]):
    __slots__ = ("iterator", "errors", "where", "replace", "do", "stop", "_stopped")

    def __init__(
        self,
        iterator: Iterator[T],
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        where: Optional[Callable[[Exception], Any]],
        replace: Optional[Callable[[Exception], U]],
        do: Optional[Callable[[Exception], Any]],
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


class _BaseGroupIterator(Generic[T]):
    __slots__ = ("iterator", "up_to", "_every_seconds", "_to_raise", "_last_yield_at")

    def __init__(
        self,
        iterator: Iterator[T],
        up_to: Optional[int],
        every: Optional[datetime.timedelta],
    ) -> None:
        self.iterator = iterator
        self.up_to = up_to or cast(int, float("inf"))
        self._every_seconds = every.total_seconds() if every else None
        self._to_raise: Optional[Exception] = None
        self._last_yield_at: float = 0

    def _every_seconds_elapsed(self) -> bool:
        if self._every_seconds is None:
            return False
        elapsed = time.perf_counter() - self._last_yield_at
        return elapsed >= self._every_seconds

    def _remember_group_time(self) -> None:
        if self._every_seconds is not None:
            self._last_yield_at = time.perf_counter()

    def _init_last_group_time(self) -> None:
        if self._every_seconds is not None and not self._last_yield_at:
            self._last_yield_at = time.perf_counter()


class GroupIterator(_BaseGroupIterator[T], Iterator[List[T]]):
    __slots__ = ("_current_group",)

    def __init__(
        self,
        iterator: Iterator[T],
        up_to: Optional[int],
        every: Optional[datetime.timedelta],
    ) -> None:
        super().__init__(iterator, up_to, every)
        self._current_group: List[T] = []

    def __next__(self) -> List[T]:
        self._init_last_group_time()
        if self._to_raise:
            try:
                raise self._to_raise
            finally:
                self._to_raise = None
        try:
            while len(self._current_group) < self.up_to and (
                not self._every_seconds_elapsed() or not self._current_group
            ):
                self._current_group.append(self.iterator.__next__())
        except Exception as e:
            if not self._current_group:
                raise
            self._to_raise = e

        group, self._current_group = self._current_group, []
        self._remember_group_time()
        return group


class GroupbyIterator(_BaseGroupIterator[T], Iterator[Tuple[U, List[T]]]):
    __slots__ = ("by", "_is_exhausted", "_current_groups")

    def __init__(
        self,
        iterator: Iterator[T],
        by: Callable[[T], U],
        up_to: Optional[int],
        every: Optional[datetime.timedelta],
    ) -> None:
        super().__init__(iterator, up_to, every)
        self.by = by
        self._is_exhausted = False
        self._current_groups: DefaultDict[U, List[T]] = defaultdict(list)

    def _group_next_elem(self) -> None:
        elem = self.iterator.__next__()
        self._current_groups[self.by(elem)].append(elem)

    def _pop_full_group(self) -> Optional[Tuple[U, List[T]]]:
        for key, group in self._current_groups.items():
            if len(group) >= self.up_to:
                return key, self._current_groups.pop(key)
        return None

    def _pop_oldest_group(self) -> Tuple[U, List[T]]:
        first_key: U = self._current_groups.__iter__().__next__()
        return first_key, self._current_groups.pop(first_key)

    def __next__(self) -> Tuple[U, List[T]]:
        self._init_last_group_time()
        if self._is_exhausted:
            if self._current_groups:
                return self._pop_oldest_group()
            raise StopIteration

        if self._to_raise:
            if self._current_groups:
                self._remember_group_time()
                return self._pop_oldest_group()
            try:
                raise self._to_raise
            finally:
                self._to_raise = None

        try:
            self._group_next_elem()

            full_group: Optional[Tuple[U, List[T]]] = self._pop_full_group()
            while not full_group and not self._every_seconds_elapsed():
                self._group_next_elem()
                full_group = self._pop_full_group()

            self._remember_group_time()
            return full_group or self._pop_oldest_group()

        except StopIteration:
            self._is_exhausted = True
            return self.__next__()

        except Exception as e:
            self._to_raise = e
            return self.__next__()


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
        do: Callable[["stream.Observation"], Any],
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

    def _observation(self) -> "stream.Observation":
        from streamable._stream import stream

        return stream.Observation(
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
        do: Callable[["stream.Observation"], Any],
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
        do: Callable[["stream.Observation"], Any],
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
        do: Callable[["stream.Observation"], Any],
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
        try:
            thread.start()
            while True:
                elem = self._buffer.get()
                if elem is STOP_ITERATION:
                    break
                self._slots.release()
                yield elem
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

            # queue, wait, yield
            while self._future_results:
                future = self._next_future()
                if future:
                    self._future_results.add(future)
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
