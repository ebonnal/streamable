import datetime
import queue
import sys
import time
import weakref
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from concurrent.futures import Executor, Future, ThreadPoolExecutor, wait
from contextlib import closing, suppress
from threading import Event, Semaphore, Thread
from typing import (
    AsyncIterable,
    Callable,
    ContextManager,
    Deque,
    Dict,
    Generator,
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

from streamable._tools._context import NoopContextManager
from streamable._tools._error import ExceptionContainer
from streamable._tools._future import (
    FailedFuture,
    FDFOFutureResults,
    FIFOFutureResults,
    FutureResults,
)
from streamable._tools._iter import (
    AsyncToSyncIterator,
    Closable,
    ClosableIterator,
    GuardedAsyncIterator,
    GuardedIterator,
)
from streamable._tools._observation import Observation
from streamable._tools._sentinel import STOP_ITERATION
from streamable._tools._validation import validate_iterable

T = TypeVar("T")
U = TypeVar("U")
Exc = TypeVar("Exc", bound=Exception)


class _BaseOperationIterator(ClosableIterator[U], Generic[T, U]):
    """
    Provides `.close` of owned upstream, and unwraps upstream if it's a `GuardedIterator`.
    """

    __slots__ = ("upstream", "_closure_requested", "_upstream_owned")

    def __init__(self, upstream: ClosableIterator[T]) -> None:
        if isinstance(upstream, GuardedIterator):
            self.upstream = upstream._upstream_iterator
            self._upstream_owned = upstream._owned
        else:
            self.upstream = upstream
            self._upstream_owned = True
        self._closure_requested = False

    def close(self) -> None:
        if not self._closure_requested:
            self._closure_requested = True
            if self._upstream_owned and isinstance(self.upstream, Closable):
                self.upstream.close()

    def _close_and_stop(self) -> None:
        try:
            raise StopIteration
        finally:
            self.close()


class _RaisingIterator(_BaseOperationIterator[Union[T, ExceptionContainer], T]):
    __slots__ = ("upstream_upstream", "_upstream_upstream_owned", "_upstream_closed")

    upstream: Generator[Union[T, ExceptionContainer], None, None]

    def __init__(
        self,
        upstream: Generator[Union[T, ExceptionContainer], None, None],
        upstream_upstream: Closable,
    ):
        super().__init__(upstream)
        self._upstream_upstream_owned = True
        self.upstream_upstream = upstream_upstream
        self._upstream_closed = False

    def __next__(self) -> T:
        return ExceptionContainer.raise_if_instance(self.upstream.__next__())

    def close(self) -> None:
        try:
            if not self._upstream_closed:
                self._upstream_closed = True
                self.upstream.close()
        finally:
            if not self._closure_requested:
                self._closure_requested = True
                if self._upstream_upstream_owned:
                    self.upstream_upstream.close()


def puller_task(
    iterator: Iterator[T],
    elems: "queue.Queue[Union[T, ExceptionContainer]]",
    limit: Semaphore,
    stopped: Event,
    base_exception: Deque[BaseException],
) -> None:
    elem: Union[T, ExceptionContainer]
    limit.acquire()
    while not stopped.is_set():
        try:
            elem = iterator.__next__()
        except StopIteration:
            elem = STOP_ITERATION
            stopped.set()
        except Exception as e:
            elem = ExceptionContainer(e)
        except BaseException as e:
            base_exception.append(e)
            elems.put_nowait(STOP_ITERATION)
            stopped.set()
            continue
        try:
            elems.put_nowait(elem)
        finally:
            del elem
        limit.acquire()


##########
# buffer #
##########


class _BufferIterable(Iterable[Union[T, ExceptionContainer]]):
    __slots__ = ("upstream", "up_to")

    def __init__(
        self,
        upstream: ClosableIterator[T],
        up_to: Optional[int],
    ) -> None:
        self.upstream = upstream
        self.up_to = up_to or sys.maxsize

    def __iter__(self) -> Generator[Union[T, ExceptionContainer], None, None]:
        buffer: "queue.Queue[Union[T, ExceptionContainer]]" = queue.Queue()
        limit = Semaphore(self.up_to)
        stopped = Event()
        base_exception: Deque[BaseException] = deque()
        thread = Thread(
            target=puller_task,
            args=(self.upstream, buffer, limit, stopped, base_exception),
            daemon=True,
        )
        to_yield: Deque[Union[T, ExceptionContainer]] = deque(maxlen=1)
        try:
            thread.start()
            while True:
                to_yield.append(buffer.get())
                if base_exception:
                    raise base_exception.pop()
                if to_yield[-1] is STOP_ITERATION:
                    break
                limit.release()
                yield to_yield.pop()
        finally:
            stopped.set()
            limit.release()
            thread.join()
            base_exception.clear()
            buffer.queue.clear()


class BufferIterator(_RaisingIterator[T]):
    __slots__ = ()

    def __init__(
        self,
        upstream: ClosableIterator[T],
        up_to: Optional[int],
    ) -> None:
        super().__init__(_BufferIterable(upstream, up_to).__iter__(), upstream)


#########
# catch #
#########


class CatchIterator(_BaseOperationIterator[T, Union[T, U]]):
    __slots__ = ("errors", "where", "replace", "do", "stop", "_stopped")

    def __init__(
        self,
        upstream: ClosableIterator[T],
        errors: Union[Type[Exc], Tuple[Type[Exc], ...]],
        where: Optional[Callable[[Exc], object]],
        replace: Optional[Callable[[Exc], U]],
        do: Optional[Callable[[Exc], object]],
        stop: bool,
    ) -> None:
        super().__init__(upstream)
        self.errors = errors
        self.where = where
        self.replace = replace
        self.do = do
        self.stop = stop
        self._stopped = False

    def __next__(self) -> Union[T, U]:
        while True:
            if self._stopped:
                self._close_and_stop()
            try:
                return self.upstream.__next__()
            except StopIteration:
                raise
            except self.errors as e:
                if self.where is None or self.where(e):
                    if self.stop:
                        self._stopped = True
                    if self.do is not None:
                        self.do(e)
                    if self.replace is not None:
                        return self.replace(e)
                    continue
                raise


###########
# flatten #
###########


class FlattenIterator(_BaseOperationIterator[Union[Iterable[T], AsyncIterable[T]], T]):
    __slots__ = ("_iterator", "_iterator_owned")

    def __init__(
        self,
        upstream: ClosableIterator[Union[Iterable[T], AsyncIterable[T]]],
    ) -> None:
        super().__init__(upstream)
        self._iterator: Iterator[T] = ().__iter__()
        self._iterator_owned: bool = False

    def __next__(self) -> T:
        while True:
            try:
                return self._iterator.__next__()
            except StopIteration:
                iterable = self.upstream.__next__()
                validate_iterable(iterable)
                if hasattr(type(iterable), "__iter__"):
                    self._iterator = iterable.__iter__()  # type: ignore
                    self._iterator_owned = not hasattr(type(iterable), "__next__")
                else:
                    iterator = cast(AsyncIterable[T], iterable).__aiter__()
                    self._iterator_owned = True
                    self._iterator = AsyncToSyncIterator(
                        iterator, owned=not hasattr(type(iterable), "__anext__")
                    )

    def close(self) -> None:
        with closing(cast(Closable, super())):
            if isinstance(self._iterator, Closable) and self._iterator_owned:
                self._iterator_owned = False
                with suppress(Exception):
                    self._iterator.close()


#########
# group #
#########


class GroupIterator(_BaseOperationIterator[T, List[T]]):
    __slots__ = ("up_to", "_group", "_to_raise")

    def __init__(
        self,
        upstream: ClosableIterator[T],
        up_to: Optional[int],
    ) -> None:
        super().__init__(upstream)
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


class GroupByIterator(_BaseOperationIterator[T, Iterable[Tuple[U, List[T]]]]):
    __slots__ = ("up_to", "by", "_groups", "_to_raise")

    def __init__(
        self,
        upstream: ClosableIterator[T],
        up_to: Optional[int],
        by: Callable[[T], U],
    ) -> None:
        super().__init__(upstream)
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
        upstream: ClosableIterator[T],
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

    def _timeout(self, groups: Dict[U, Tuple[float, List[T]]]) -> Optional[float]:
        if groups:
            oldest_group_time = next(iter(groups.values()))[0]
            timeout = oldest_group_time + self.within_seconds - time.perf_counter()
            return max(0, timeout)
        return None

    def __iter__(
        self,
    ) -> Generator[Union[ExceptionContainer, Tuple[U, List[T]]], None, None]:
        next_elem: "queue.Queue[Union[T, ExceptionContainer]]" = queue.Queue()
        limit = Semaphore(0)
        stopped = Event()
        base_exception: Deque[BaseException] = deque()
        thread = Thread(
            target=puller_task,
            args=(self.upstream, next_elem, limit, stopped, base_exception),
            daemon=True,
        )
        groups: Dict[U, Tuple[float, List[T]]] = defaultdict(
            lambda: (time.perf_counter(), [])
        )
        try:
            thread.start()
            while True:
                limit.release()
                try:
                    while True:
                        try:
                            elem = next_elem.get(timeout=self._timeout(groups))
                        except queue.Empty:
                            yield self._oldest_group(groups)
                            continue
                        if base_exception:
                            raise base_exception.pop()
                        elem = ExceptionContainer.raise_if_instance(elem)
                        if elem is STOP_ITERATION:
                            while groups:
                                yield self._oldest_group(groups)
                            return
                        # elem received from upstream, exit the timeout loop
                        break
                    # add elem to group
                    key = self.by(elem)
                    _, group = groups[key]
                    group.append(elem)
                    # yield group if full
                    if len(group) == self.up_to:
                        del groups[key]
                        yield (key, group)
                    # get the next upstream elem
                    continue
                except Exception as e:
                    # upstream or `by` raised an error, yield all groups
                    while groups:
                        yield self._oldest_group(groups)
                    to_raise = [ExceptionContainer(e)]
                # yield outside the except block so the frame's exception state is cleared
                yield to_raise.pop()
        finally:
            stopped.set()
            limit.release()
            thread.join()
            base_exception.clear()
            next_elem.queue.clear()


class GroupByWithinIterator(_RaisingIterator[Tuple[U, List[T]]]):
    __slots__ = ()

    def __init__(
        self,
        upstream: ClosableIterator[T],
        up_to: Optional[int],
        by: Callable[[T], U],
        within: datetime.timedelta,
    ) -> None:
        super().__init__(
            iter(_GroupByWithinIterable(upstream, up_to, by, within)), upstream
        )


########
# skip #
########


class CountSkipIterator(_BaseOperationIterator[T, T]):
    __slots__ = "_remaining_to_skip"

    def __init__(self, upstream: ClosableIterator[T], count: int) -> None:
        super().__init__(upstream)
        self._remaining_to_skip = count

    def __next__(self) -> T:
        while self._remaining_to_skip > 0:
            self.upstream.__next__()
            # do not count exceptions as skipped elements
            self._remaining_to_skip -= 1
        return self.upstream.__next__()


class PredicateSkipIterator(_BaseOperationIterator[T, T]):
    __slots__ = ("until", "_satisfied")

    def __init__(
        self, upstream: ClosableIterator[T], until: Callable[[T], object]
    ) -> None:
        super().__init__(upstream)
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


class CountTakeIterator(_BaseOperationIterator[T, T]):
    __slots__ = "_remaining_to_take"

    def __init__(self, upstream: ClosableIterator[T], count: int) -> None:
        super().__init__(upstream)
        self._remaining_to_take = count

    def __next__(self) -> T:
        if self._remaining_to_take <= 0:
            self._close_and_stop()
        elem = self.upstream.__next__()
        self._remaining_to_take -= 1
        return elem


class PredicateTakeIterator(_BaseOperationIterator[T, T]):
    __slots__ = ("until", "_satisfied")

    def __init__(
        self, upstream: ClosableIterator[T], until: Callable[[T], object]
    ) -> None:
        super().__init__(upstream)
        self.until = until
        self._satisfied = False

    def __next__(self) -> T:
        if self._satisfied:
            self._close_and_stop()
        elem = self.upstream.__next__()
        if self.until(elem):
            self._satisfied = True
            return self.__next__()
        return elem


#######
# map #
#######


class MapIterator(map, ClosableIterator[U], Generic[T, U]):
    __slots__ = ("upstream", "_upstream_owned", "_closure_requested", "into")
    upstream: Iterator[T]
    _closure_requested: bool
    _upstream_owned: bool
    into: Callable[[T], U]

    def __new__(cls, into: Callable[[T], U], upstream: Iterator[T]) -> "MapIterator":
        upstream_owned = True
        if isinstance(upstream, GuardedIterator):
            upstream_owned = upstream._owned
            upstream = upstream._upstream_iterator
        self = super().__new__(cls, into, upstream)
        self.upstream = upstream
        self._upstream_owned = upstream_owned
        self._closure_requested = False
        self.into = into
        return self

    def close(self) -> None:
        if (
            not self._closure_requested
            and self._upstream_owned
            and isinstance(self.upstream, Closable)
        ):
            self._closure_requested = True
            self.upstream.close()


##########
# filter #
##########


class FilterIterator(filter, ClosableIterator[T]):
    __slots__ = ("upstream", "_upstream_owned", "_closure_requested", "where")
    upstream: Iterator[T]
    _closure_requested: bool
    _upstream_owned: bool
    where: Callable[[T], object]

    def __new__(
        cls, where: Callable[[T], object], upstream: Iterator[T]
    ) -> "FilterIterator":
        upstream_owned = True
        if isinstance(upstream, GuardedIterator):
            upstream_owned = upstream._owned
            upstream = upstream._upstream_iterator
        self = super().__new__(cls, where, upstream)
        self.upstream = upstream
        self._upstream_owned = upstream_owned
        self._closure_requested = False
        self.where = where
        return self

    def close(self) -> None:
        if (
            not self._closure_requested
            and self._upstream_owned
            and isinstance(self.upstream, Closable)
        ):
            self._closure_requested = True
            self.upstream.close()


###########
# observe #
###########


class _BaseObserveIterator(_BaseOperationIterator[T, T]):
    __slots__ = (
        "subject",
        "do",
        "_elements",
        "_errors",
        "_emissions_observed",
        "_elements_observed",
        "_errors_observed",
        "_activated",
        "_start_point",
        "_do_base_exception",
        "_stopped",
    )

    def __init__(
        self,
        upstream: ClosableIterator[T],
        subject: str,
        do: Callable[[Observation], object],
    ) -> None:
        super().__init__(upstream)
        self.subject = subject
        self.do = do
        self._elements = 0
        self._errors = 0
        self._emissions_observed = 0
        self._elements_observed = 0
        self._errors_observed = 0
        self._activated = False
        self._start_point: float
        self._do_base_exception: Optional[BaseException] = None
        self._stopped = Event()

    @property
    def _emissions(self) -> int:
        return self._elements + self._errors

    def _observation(self) -> Observation:
        return Observation(
            subject=self.subject,
            elapsed=datetime.timedelta(seconds=time.perf_counter() - self._start_point),
            errors=self._errors,
            elements=self._elements,
        )

    def _activate(self) -> None:
        self._start_point = time.perf_counter()
        self._activated = True

    def _observe(self) -> None:
        self._emissions_observed = self._emissions
        try:
            self.do(self._observation())
        except Exception:
            pass
        except BaseException as e:
            self._do_base_exception = e

    def _reraise_do_base_exception(self) -> None:
        if self._do_base_exception:
            try:
                raise self._do_base_exception
            finally:
                self._do_base_exception = None

    @abstractmethod
    def _threshold(self, observed: int) -> int: ...

    def __next__(self) -> T:
        if not self._activated:
            self._activate()
        try:
            elem = self.upstream.__next__()
            self._elements += 1
            if self._elements >= self._threshold(self._elements_observed):
                self._observe()
                self._elements_observed = self._elements
            return elem
        except StopIteration:
            if not self._stopped.is_set() and (
                not self._emissions or self._emissions > self._emissions_observed
            ):
                self._observe()
            self._stopped.set()
            raise
        except Exception:
            self._errors += 1
            if self._errors >= self._threshold(self._errors_observed):
                self._observe()
                self._errors_observed = self._errors
            raise
        finally:
            self._reraise_do_base_exception()


class PowerObserveIterator(_BaseObserveIterator[T]):
    __slots__ = ("base",)

    def __init__(
        self,
        upstream: ClosableIterator[T],
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
        upstream: ClosableIterator[T],
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
    __slots__ = ("__weakref__", "every", "_thread_is_done")

    def __init__(
        self,
        upstream: ClosableIterator[T],
        subject: str,
        every: datetime.timedelta,
        do: Callable[[Observation], object],
    ) -> None:
        super().__init__(upstream, subject, do)
        self.every = every
        self._thread_is_done = Event()

    @staticmethod
    def _observer(
        weak_self: "weakref.ReferenceType[EveryIntervalObserveIterator[T]]",
        every_seconds: float,
        stopped: Event,
        thread_is_done: Event,
    ) -> None:
        self = weak_self()
        while self and not stopped.is_set():
            self._observe()
            if self._do_base_exception:
                break
            self = None
            stopped.wait(timeout=every_seconds)
            self = weak_self()
        thread_is_done.set()

    def _activate(self) -> None:
        super()._activate()
        Thread(
            target=self._observer,
            args=(
                weakref.ref(self),
                self.every.total_seconds(),
                self._stopped,
                self._thread_is_done,
            ),
            daemon=True,
        ).start()

    def _threshold(self, observed: int) -> int:
        return cast(int, float("inf"))

    def close(self) -> None:
        with closing(cast(Closable, super())):
            self._stopped.set()
            if self._activated:
                self._thread_is_done.wait()

    def __del__(self) -> None:
        self._stopped.set()


############
# throttle #
############


class ThrottleIterator(_BaseOperationIterator[T, T]):
    __slots__ = ("up_to", "_window_seconds", "_emission_timestamps")

    def __init__(
        self,
        upstream: ClosableIterator[T],
        up_to: int,
        per: datetime.timedelta,
    ) -> None:
        super().__init__(upstream)
        self.up_to = up_to
        self._window_seconds = per.total_seconds()
        self._emission_timestamps: Deque[float] = deque()

    def _sleep(self) -> None:
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

    def __next__(self) -> T:
        try:
            elem = self.upstream.__next__()
            self._sleep()
            return elem
        except StopIteration:
            raise
        except Exception:
            self._sleep()
            raise


##################
# concurrent map #
##################


class _ConcurrentMapIterable(
    Generic[T, U], ABC, Iterable[Union[U, ExceptionContainer]]
):
    __slots__ = ("upstream", "into", "concurrency", "as_completed", "_executor")

    def __init__(
        self,
        upstream: ClosableIterator[T],
        into: Callable[[T], U],
        concurrency: Union[int, Executor],
        as_completed: bool,
    ) -> None:
        self.upstream = upstream
        self.into = into
        self.as_completed = as_completed
        self._executor: Optional[Executor] = None
        if isinstance(concurrency, int):
            self.concurrency = concurrency
        else:
            self._executor = concurrency
            self.concurrency = getattr(self._executor, "_max_workers")

    def _launch_task(self, elem: T, context: Executor) -> "Future[U]":
        return context.submit(self.into, elem)

    def _task_context(self) -> ContextManager[Executor]:
        if self._executor:
            # avoid closing client's executor
            return NoopContextManager(self._executor)
        return ThreadPoolExecutor(max_workers=self.concurrency)

    def _next_future(self, context: Executor) -> Optional["Future[U]"]:
        try:
            elem = self.upstream.__next__()
        except StopIteration:
            return None
        except Exception as e:
            return FailedFuture(e)
        return self._launch_task(elem, context)

    def __iter__(self) -> Generator[Union[U, ExceptionContainer], None, None]:
        future_results: FutureResults[U] = (
            FDFOFutureResults() if self.as_completed else FIFOFutureResults()
        )
        with self._task_context() as executor:
            try:
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
            finally:
                future_results.cancel()
                future_results.clear()


class ConcurrentMapIterator(_RaisingIterator[U]):
    __slots__ = ()

    def __init__(
        self,
        upstream: ClosableIterator[T],
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
            ).__iter__(),
            upstream,
        )


######################
# concurrent flatten #
######################


class _ConcurrentFlattenIterable(Iterable[Union[T, ExceptionContainer]]):
    __slots__ = ("upstream", "concurrency")

    def __init__(
        self,
        upstream: ClosableIterator[Union[Iterable[T], AsyncIterable[T]]],
        concurrency: int,
    ) -> None:
        self.upstream = upstream
        self.concurrency = concurrency

    def __iter__(self) -> Generator[Union[T, ExceptionContainer], None, None]:
        iterator_and_future_pairs: Deque[
            Tuple[
                Optional[ClosableIterator[T]],
                "Future[T]",
            ]
        ] = deque()
        to_yield: Deque[Union[T, ExceptionContainer]] = deque(maxlen=1)
        iterator_to_queue: Optional[ClosableIterator[T]] = None

        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            try:
                # wait, queue, yield (FIFO)
                while True:
                    if iterator_and_future_pairs:
                        elem = ExceptionContainer.result(
                            iterator_and_future_pairs[0][1]
                        )
                        iterator = iterator_and_future_pairs.popleft()[0]
                        if elem is not STOP_ITERATION:
                            to_yield.append(elem)
                            iterator_to_queue = iterator
                        del elem

                    # queue tasks up to buffersize
                    while len(iterator_and_future_pairs) < self.concurrency:
                        if iterator_to_queue is None:
                            try:
                                try:
                                    iterable = self.upstream.__next__()
                                except StopIteration:
                                    break
                                validate_iterable(iterable)
                                if isinstance(iterable, Iterable):
                                    iterator_to_queue = GuardedIterator(
                                        iterable, silent_close=True
                                    )
                                else:
                                    iterator_to_queue = AsyncToSyncIterator(
                                        GuardedAsyncIterator(
                                            iterable, silent_close=True
                                        )
                                    )
                            except Exception as e:
                                iterator_to_queue = None
                                iterator_and_future_pairs.append(
                                    (iterator_to_queue, FailedFuture(e))
                                )
                                continue
                        future = executor.submit(
                            next, iterator_to_queue, STOP_ITERATION
                        )
                        iterator_and_future_pairs.append((iterator_to_queue, future))
                        iterator_to_queue = None
                    if to_yield:
                        yield to_yield.pop()
                    if not iterator_and_future_pairs:
                        break
            finally:
                if iterator_and_future_pairs:
                    iterators: Tuple[Optional[Closable], ...]
                    futures: Tuple["Future[T]", ...]
                    iterators, futures = zip(*iterator_and_future_pairs)
                    iterator_and_future_pairs.clear()
                    list(fut.cancel() for fut in futures)
                    wait(futures)
                    del futures
                    wait([executor.submit(it.close) for it in iterators if it])


class ConcurrentFlattenIterator(_RaisingIterator[T]):
    __slots__ = ()

    def __init__(
        self,
        upstream: ClosableIterator[Union[Iterable[T], AsyncIterable[T]]],
        concurrency: int,
    ) -> None:
        super().__init__(
            _ConcurrentFlattenIterable(
                upstream,
                concurrency,
            ).__iter__(),
            upstream,
        )
