import datetime
import queue
import sys
from threading import Event, Semaphore, Thread
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque

from concurrent.futures import Executor, Future, ThreadPoolExecutor
from contextlib import suppress
from typing import (
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

from streamable._tools._context import NoopContextManager
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
    __slots__ = ("upstream", "up_to")

    def __init__(
        self,
        upstream: Iterator[T],
        up_to: Optional[int],
    ) -> None:
        self.upstream = upstream
        self.up_to = up_to or sys.maxsize

    def _buffer_upstream(
        self,
        buffer: "queue.Queue[Union[T, ExceptionContainer]]",
        slots: Semaphore,
        stopped: Event,
    ) -> None:
        elem: Union[T, ExceptionContainer]
        slots.acquire()
        while not stopped.is_set():
            try:
                elem = self.upstream.__next__()
            except StopIteration:
                elem = STOP_ITERATION
                stopped.set()
            except Exception as e:
                elem = ExceptionContainer(e)
            buffer.put_nowait(elem)
            slots.acquire()

    def __iter__(self) -> Iterator[Union[T, ExceptionContainer]]:
        buffer: "queue.Queue[Union[T, ExceptionContainer]]" = queue.Queue()
        slots = Semaphore(self.up_to)
        stopped = Event()
        thread = Thread(
            target=self._buffer_upstream,
            args=(buffer, slots, stopped),
            daemon=True,
        )
        to_yield: Deque[Union[T, ExceptionContainer]] = deque(maxlen=1)
        try:
            thread.start()
            while True:
                to_yield.append(buffer.get())
                if to_yield[-1] is STOP_ITERATION:
                    break
                slots.release()
                yield to_yield.pop()
        finally:
            stopped.set()
            slots.release()
            thread.join()


class BufferIterator(RaisingIterator[T]):
    __slots__ = ()

    def __init__(
        self,
        upstream: Iterator[T],
        up_to: Optional[int],
    ) -> None:
        super().__init__(_BufferIterable(upstream, up_to).__iter__())


#########
# catch #
#########


class CatchIterator(Iterator[Union[T, U]]):
    __slots__ = ("upstream", "errors", "where", "replace", "do", "stop", "_stopped")

    def __init__(
        self,
        upstream: Iterator[T],
        errors: Union[Type[Exc], Tuple[Type[Exc], ...]],
        where: Optional[Callable[[Exc], object]],
        replace: Optional[Callable[[Exc], U]],
        do: Optional[Callable[[Exc], object]],
        stop: bool,
    ) -> None:
        self.upstream = upstream
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
                return self.upstream.__next__()
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
    __slots__ = ("upstream", "_current_iterator_elem")

    def __init__(
        self,
        upstream: Iterator[Iterable[T]],
    ) -> None:
        self.upstream = upstream
        self._current_iterator_elem: Iterator[T] = ().__iter__()

    def __next__(self) -> T:
        while True:
            try:
                return self._current_iterator_elem.__next__()
            except StopIteration:
                iterable = self.upstream.__next__()
                validate_sync_flatten_iterable(iterable)
                self._current_iterator_elem = iterable.__iter__()


#########
# group #
#########


class GroupIterator(Iterator[List[T]]):
    __slots__ = ("upstream", "up_to", "_group", "_to_raise")

    def __init__(
        self,
        upstream: Iterator[T],
        up_to: Optional[int],
    ) -> None:
        self.upstream = upstream
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
                self._group.append(self.upstream.__next__())
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
    __slots__ = ("upstream", "up_to", "by", "_groups", "_to_raise")

    def __init__(
        self,
        upstream: Iterator[T],
        up_to: Optional[int],
        by: Callable[[T], U],
    ) -> None:
        self.upstream = upstream
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
                elem = self.upstream.__next__()
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
    __slots__ = ("upstream", "up_to", "by", "within_seconds")

    def __init__(
        self,
        upstream: Iterator[T],
        up_to: Optional[int],
        by: Callable[[T], U],
        within: datetime.timedelta,
    ) -> None:
        self.upstream = upstream
        self.up_to = up_to or cast(int, float("inf"))
        self.by = by
        self.within_seconds = within.total_seconds()

    @staticmethod
    def _oldest_group(groups: Dict[U, Tuple[float, List[T]]]) -> Tuple[U, List[T]]:
        oldest_key = next(iter(groups.keys()))
        return (oldest_key, groups.pop(oldest_key)[1])

    @staticmethod
    def _get_next_elem(
        next_elem: "queue.Queue[Union[T, ExceptionContainer]]",
        timeout: Optional[float],
    ) -> T:
        elem = next_elem.get(timeout=timeout)
        if elem is STOP_ITERATION:
            raise StopIteration
        if isinstance(elem, ExceptionContainer):
            try:
                raise elem.exception
            finally:
                del elem
        return elem

    def _timeout(self, groups: Dict[U, Tuple[float, List[T]]]) -> Optional[float]:
        if groups:
            oldest_group_time = next(iter(groups.values()))[0]
            timeout = oldest_group_time + self.within_seconds - time.perf_counter()
            return max(0, timeout)
        return None

    def _puller(
        self,
        next_elem: "queue.Queue[Union[T, ExceptionContainer]]",
        let_pull_next: Semaphore,
        stopped: Event,
    ) -> None:
        elem: Union[T, ExceptionContainer]
        let_pull_next.acquire()
        while not stopped.is_set():
            try:
                elem = self.upstream.__next__()
            except StopIteration:
                elem = STOP_ITERATION
                stopped.set()
            except Exception as e:
                elem = ExceptionContainer(e)
            try:
                next_elem.put_nowait(elem)
            finally:
                del elem
            let_pull_next.acquire()

    def __iter__(self) -> Iterator[Union[ExceptionContainer, Tuple[U, List[T]]]]:
        groups: Dict[U, Tuple[float, List[T]]] = defaultdict(
            lambda: (time.perf_counter(), [])
        )
        next_elem: "queue.Queue[Union[T, ExceptionContainer]]" = queue.Queue()
        let_pull_next = Semaphore(0)
        stopped = Event()
        thread = Thread(
            target=self._puller,
            args=(next_elem, let_pull_next, stopped),
            daemon=True,
        )
        try:
            thread.start()
            while True:
                let_pull_next.release()
                try:
                    while True:
                        try:
                            elem = self._get_next_elem(
                                next_elem, timeout=self._timeout(groups)
                            )
                            break
                        except queue.Empty:
                            yield self._oldest_group(groups)
                    key = self.by(elem)
                    _, group = groups[key]
                    group.append(elem)
                    if len(group) == self.up_to:
                        del groups[key]
                        yield (key, group)
                    continue
                except Exception as e:
                    # upstream stopped iteration, or raised an error, or `by` did
                    while groups:
                        yield self._oldest_group(groups)
                    if isinstance(e, StopIteration):
                        return
                    error = [ExceptionContainer(e)]
                # yield outside the except block so the frame's exception state is cleared
                yield error.pop()
        finally:
            stopped.set()
            let_pull_next.release()
            thread.join()


class GroupByWithinIterator(RaisingIterator[Tuple[U, List[T]]]):
    def __init__(
        self,
        upstream: Iterator[T],
        up_to: Optional[int],
        by: Callable[[T], U],
        within: datetime.timedelta,
    ) -> None:
        super().__init__(iter(_GroupByWithinIterable(upstream, up_to, by, within)))


########
# skip #
########


class CountSkipIterator(Iterator[T]):
    __slots__ = ("upstream", "_remaining_to_skip")

    def __init__(self, upstream: Iterator[T], count: int) -> None:
        self.upstream = upstream
        self._remaining_to_skip = count

    def __next__(self) -> T:
        while self._remaining_to_skip > 0:
            self.upstream.__next__()
            # do not count exceptions as skipped elements
            self._remaining_to_skip -= 1
        return self.upstream.__next__()


class PredicateSkipIterator(Iterator[T]):
    __slots__ = ("upstream", "until", "_satisfied")

    def __init__(self, upstream: Iterator[T], until: Callable[[T], object]) -> None:
        self.upstream = upstream
        self.until = until
        self._satisfied = False

    def __next__(self) -> T:
        elem = self.upstream.__next__()
        if not self._satisfied:
            while not self.until(elem):
                elem = self.upstream.__next__()
            self._satisfied = True
        return elem


############
# take #
############


class CountTakeIterator(Iterator[T]):
    __slots__ = ("upstream", "_remaining_to_take")

    def __init__(self, upstream: Iterator[T], count: int) -> None:
        self.upstream = upstream
        self._remaining_to_take = count

    def __next__(self) -> T:
        if self._remaining_to_take <= 0:
            raise StopIteration
        elem = self.upstream.__next__()
        self._remaining_to_take -= 1
        return elem


class PredicateTakeIterator(Iterator[T]):
    __slots__ = ("upstream", "until", "_satisfied")

    def __init__(self, upstream: Iterator[T], until: Callable[[T], object]) -> None:
        self.upstream = upstream
        self.until = until
        self._satisfied = False

    def __next__(self) -> T:
        if self._satisfied:
            raise StopIteration
        elem = self.upstream.__next__()
        if self.until(elem):
            self._satisfied = True
            return self.__next__()
        return elem


###########
# observe #
###########


class _BaseObserveIterator(Iterator[T]):
    __slots__ = (
        "upstream",
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
        upstream: Iterator[T],
        subject: str,
        do: Callable[[Observation], object],
    ) -> None:
        self.upstream = upstream
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
            elem = self.upstream.__next__()
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
        upstream: Iterator[T],
        subject: str,
        do: Callable[[Observation], object],
        base: int = 2,
    ) -> None:
        super().__init__(upstream, subject, do)
        self.base = base

    def _threshold(self, observed: int) -> int:
        return self.base * observed


class EveryIntObserveIterator(_BaseObserveIterator[T]):
    __slots__ = ("every",)

    def __init__(
        self,
        upstream: Iterator[T],
        subject: str,
        every: int,
        do: Callable[[Observation], object],
    ) -> None:
        super().__init__(upstream, subject, do)
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
        upstream: Iterator[T],
        subject: str,
        every: datetime.timedelta,
        do: Callable[[Observation], object],
    ) -> None:
        super().__init__(upstream, subject, do)
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
    __slots__ = ("upstream", "up_to", "_window_seconds", "_emission_timestamps")

    def __init__(
        self,
        upstream: Iterator[T],
        up_to: int,
        per: datetime.timedelta,
    ) -> None:
        self.upstream = upstream
        self.up_to = up_to
        self._window_seconds = per.total_seconds()
        self._emission_timestamps: Deque[float] = deque()

    def __next__(self) -> T:
        elem: Optional[T] = None
        error: Optional[Exception] = None
        try:
            elem = self.upstream.__next__()
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
    __slots__ = ("upstream", "into", "concurrency", "as_completed", "_executor")

    def __init__(
        self,
        upstream: Iterator[T],
        into: Callable[[T], U],
        concurrency: Union[int, Executor],
        as_completed: bool,
    ) -> None:
        self.upstream = upstream
        self.into = ExceptionContainer.wrap(into)
        self.as_completed = as_completed
        self._executor: Optional[Executor] = None
        if isinstance(concurrency, int):
            self.concurrency = concurrency
        else:
            self._executor = concurrency
            self.concurrency = getattr(self._executor, "_max_workers")

    def _launch_task(
        self, elem: T, context: Executor
    ) -> "Future[Union[U, ExceptionContainer]]":
        return context.submit(self.into, elem)

    def _task_context(self) -> ContextManager[Executor]:
        if self._executor:
            # avoid closing client's executor
            return NoopContextManager(self._executor)
        return ThreadPoolExecutor(max_workers=self.concurrency)

    def _next_future(
        self, context: Executor
    ) -> Optional["Future[Union[U, ExceptionContainer]]"]:
        try:
            elem = self.upstream.__next__()
        except StopIteration:
            return None
        except Exception as e:
            return FutureResult(ExceptionContainer(e))
        return self._launch_task(elem, context)

    def __iter__(self) -> Iterator[Union[U, ExceptionContainer]]:
        with self._task_context() as executor:
            future_results: FutureResults[Union[U, ExceptionContainer]] = (
                FDFOFutureResults() if self.as_completed else FIFOFutureResults()
            )
            # queue tasks up to buffersize
            while len(future_results) < self.concurrency:
                future = self._next_future(executor)
                if not future:
                    # no more tasks to queue
                    break
                future_results.add(future)
                del future

            # queue, wait, yield
            while future_results:
                future = self._next_future(executor)
                if future:
                    future_results.add(future)
                    del future
                yield future_results.__next__()


class ConcurrentMapIterator(RaisingIterator[U]):
    __slots__ = ()

    def __init__(
        self,
        upstream: Iterator[T],
        into: Callable[[T], U],
        concurrency: Union[int, Executor],
        as_completed: bool,
    ) -> None:
        super().__init__(
            _ConcurrentMapIterable(
                upstream,
                into,
                concurrency,
                as_completed,
            ).__iter__()
        )


######################
# concurrent flatten #
######################


class _ConcurrentFlattenIterable(Iterable[Union[T, ExceptionContainer]]):
    __slots__ = ("upstream", "concurrency")

    def __init__(
        self,
        upstream: Iterator[Iterable[T]],
        concurrency: int,
    ) -> None:
        self.upstream = upstream
        self.concurrency = concurrency

    def __iter__(self) -> Iterator[Union[T, ExceptionContainer]]:
        safe_next = ExceptionContainer.wrap(next)
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
                                iterable = self.upstream.__next__()
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
                    future = executor.submit(safe_next, iterator_to_queue)
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
        upstream: Iterator[Iterable[T]],
        concurrency: int,
    ) -> None:
        super().__init__(
            _ConcurrentFlattenIterable(
                upstream,
                concurrency,
            ).__iter__()
        )
