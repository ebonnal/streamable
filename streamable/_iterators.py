import datetime
import queue
import sys
import time
import weakref
from abc import abstractmethod
from collections import defaultdict, deque
from concurrent.futures import Executor, Future, ThreadPoolExecutor, wait
from contextlib import closing, suppress
from threading import Event, Semaphore, Thread
from typing import (
    AsyncIterable,
    Callable,
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


def puller_task(
    iterator: Iterator[T],
    elems: "queue.Queue[Union[T, ExceptionContainer]]",
    limit: Semaphore,
    stop: Event,
    done: Event,
) -> None:
    try:
        limit.acquire()
        while not stop.is_set():
            try:
                elems.put_nowait(iterator.__next__())
            except StopIteration:
                elems.put_nowait(STOP_ITERATION)
                break
            except BaseException as e:
                elems.put_nowait(ExceptionContainer(e))
            limit.acquire()
    finally:
        done.set()


class _PullerBasedIterator(_BaseOperationIterator[T, U]):
    __slots__ = (
        "_pulled",
        "_limit",
        "_stopped",
        "_puller",
        "_stop_puller",
        "_puller_done",
    )

    def __init__(
        self,
        upstream: ClosableIterator[T],
        ahead_pulls: int,
    ) -> None:
        super().__init__(upstream)
        self._pulled: "queue.Queue[Union[T, ExceptionContainer]]" = queue.Queue()
        self._limit = Semaphore(ahead_pulls)
        self._stopped: bool = False
        self._puller: Optional[Thread] = None
        self._stop_puller = Event()
        self._puller_done = Event()

    def _start_puller(self) -> None:
        if not self._puller:
            self._puller = Thread(
                target=puller_task,
                args=(
                    self.upstream,
                    self._pulled,
                    self._limit,
                    self._stop_puller,
                    self._puller_done,
                ),
                daemon=True,
            )
        if not self._puller.ident:
            self._puller.start()

    def _terminate(self) -> None:
        self._stopped = True
        self._stop_puller.set()
        self._limit.release()
        if self._puller and self._puller.ident:
            self._puller_done.wait()

    def close(self) -> None:
        with closing(cast(Closable, super())):
            self._terminate()

    def __del__(self) -> None:
        self._stop_puller.set()
        self._limit.release()


##########
# buffer #
##########


class BufferIterator(_PullerBasedIterator[T, T]):
    __slots__ = ()

    def __init__(
        self,
        upstream: ClosableIterator[T],
        up_to: Optional[int],
    ) -> None:
        super().__init__(upstream, up_to or sys.maxsize)

    def __next__(self) -> T:
        if self._stopped:
            raise StopIteration
        self._start_puller()
        try:
            elem = [self._pulled.get()]
        finally:
            self._limit.release()
        if elem[-1] is STOP_ITERATION:
            self._terminate()
            raise StopIteration
        return ExceptionContainer.raise_if_instance(elem.pop())


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
        if self._to_raise is not None:
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
        if self._to_raise is not None:
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


class GroupByWithinIterator(_PullerBasedIterator[T, Tuple[U, List[T]]]):
    __slots__ = (
        "up_to",
        "by",
        "within_seconds",
        "_groups",
        "_to_raise",
        "_release_needed",
    )

    def __init__(
        self,
        upstream: ClosableIterator[T],
        up_to: Optional[int],
        by: Callable[[T], U],
        within: datetime.timedelta,
    ) -> None:
        super().__init__(upstream, 0)
        self.up_to = up_to or cast(int, float("inf"))
        self.by = by
        self.within_seconds = within.total_seconds()
        self._groups: Dict[U, Tuple[float, List[T]]] = defaultdict(
            lambda: (time.perf_counter(), [])
        )
        self._to_raise: Optional[Exception] = None
        self._release_needed: bool = True

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

    def __next__(self) -> Tuple[U, List[T]]:
        if self._to_raise is not None:
            if self._groups:
                return self._oldest_group(self._groups)
            try:
                raise self._to_raise
            finally:
                self._to_raise = None
        if self._stopped:
            raise StopIteration
        self._start_puller()
        while True:
            try:
                if self._release_needed:
                    self._limit.release()
                    self._release_needed = False
                try:
                    elem = self._pulled.get(timeout=self._timeout(self._groups))
                except queue.Empty:
                    return self._oldest_group(self._groups)
                except BaseException:
                    self._release_needed = True
                    raise
                else:
                    self._release_needed = True
                elem = ExceptionContainer.raise_if_instance(elem)
                if elem is STOP_ITERATION:
                    self._terminate()
                    raise StopIteration
                key = self.by(elem)
                _, group = self._groups[key]
                group.append(elem)
                if len(group) == self.up_to:
                    del self._groups[key]
                    return (key, group)
            except Exception as e:
                if self._groups:
                    self._to_raise = e
                    return self.__next__()
                raise


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
        if self._do_base_exception is not None:
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


##########################
# concurrent map/flatten #
##########################


class _ExecutorBackedIterator(_BaseOperationIterator[T, U]):
    __slots__ = (
        "concurrency",
        "_executor",
        "_stopped",
        "_executor_owned",
    )

    def __init__(
        self,
        upstream: ClosableIterator[T],
        concurrency: Union[int, Executor],
    ) -> None:
        super().__init__(upstream)

        self.concurrency: int
        self._executor: Optional[Executor] = None
        self._stopped: bool = False
        self._executor_owned: bool
        if isinstance(concurrency, int):
            self.concurrency = concurrency
            self._executor_owned = True
        else:
            self._executor = concurrency
            self.concurrency = getattr(self._executor, "_max_workers")
            self._executor_owned = False

    @property
    def _lazy_executor(self) -> Executor:
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=self.concurrency)
        return self._executor

    def _shutdown_executor(self) -> None:
        if self._executor and self._executor_owned:
            self._executor.shutdown()

    @abstractmethod
    def _terminate(self) -> None: ...
    @abstractmethod
    def _cancel(self) -> None: ...

    def close(self) -> None:
        with closing(cast(Closable, super())):
            self._terminate()

    def __del__(self) -> None:
        self._cancel()


class ConcurrentMapIterator(_ExecutorBackedIterator[T, U]):
    __slots__ = ("into", "as_completed", "_future_results")

    def __init__(
        self,
        upstream: ClosableIterator[T],
        into: Callable[[T], U],
        concurrency: Union[int, Executor],
        as_completed: bool,
    ) -> None:
        self.into = into
        self.as_completed = as_completed
        self._future_results: FutureResults[U] = (
            FDFOFutureResults() if self.as_completed else FIFOFutureResults()
        )
        super().__init__(upstream, concurrency)

    def _next_future(self) -> Optional["Future[U]"]:
        try:
            elem = self.upstream.__next__()
        except StopIteration:
            return None
        except Exception as e:
            return FailedFuture(e)
        return self._lazy_executor.submit(self.into, elem)

    def __next__(self) -> U:
        if self._stopped:
            raise StopIteration
        while len(self._future_results) < self.concurrency + 1:
            future = self._next_future()
            if future is None:
                # no more tasks to queue
                break
            self._future_results.add(future)
            del future
        if not self._future_results:
            self._terminate()
            return self.__next__()
        return ExceptionContainer.raise_if_instance(self._future_results.__next__())

    def _terminate(self) -> None:
        self._stopped = True
        self._future_results.cancel()
        self._future_results.gather()
        self._future_results.clear()
        self._shutdown_executor()

    def _cancel(self) -> None:
        self._future_results.cancel()


class ConcurrentFlattenIterator(
    _ExecutorBackedIterator[Union[Iterable[T], AsyncIterable[T]], T]
):
    __slots__ = ("_iterators_and_futures",)

    def __init__(
        self,
        upstream: ClosableIterator[Union[Iterable[T], AsyncIterable[T]]],
        concurrency: Union[int, Executor],
    ) -> None:
        self._iterators_and_futures: Deque[
            Tuple[Optional[ClosableIterator[T]], "Future[T]"]
        ] = deque()
        super().__init__(upstream, concurrency)

    def _launch_future_from_iter(self, iterator: ClosableIterator[T]) -> "Future[T]":
        return self._lazy_executor.submit(next, iterator, STOP_ITERATION)

    def _launch_future_from_next_iter(
        self,
    ) -> Optional[Tuple[Optional[ClosableIterator[T]], "Future[T]"]]:
        try:
            iterable = self.upstream.__next__()
        except StopIteration:
            return None
        except Exception as e:
            return (None, FailedFuture(e))
        validate_iterable(iterable)
        iterator: ClosableIterator[T] = (
            GuardedIterator(iterable, silent_close=True)
            if isinstance(iterable, Iterable)
            else AsyncToSyncIterator(GuardedAsyncIterator(iterable, silent_close=True))
        )
        return (iterator, self._launch_future_from_iter(iterator))

    def __next__(self) -> T:
        while True:
            if self._stopped:
                raise StopIteration
            while len(self._iterators_and_futures) < self.concurrency:
                iterator_and_future = self._launch_future_from_next_iter()
                if iterator_and_future is None:
                    # no more tasks to queue
                    break
                self._iterators_and_futures.append(iterator_and_future)
            if not self._iterators_and_futures:
                self._terminate()
                continue
            iterator, future = self._iterators_and_futures[0]
            wait((future,))
            self._iterators_and_futures.popleft()
            try:
                elem = future.result()
                if elem is STOP_ITERATION:
                    iterator = None
                    continue
                return elem
            finally:
                if iterator is not None:
                    self._iterators_and_futures.append(
                        (iterator, self._launch_future_from_iter(iterator))
                    )

    def _terminate(self) -> None:
        self._stopped = True
        if self._iterators_and_futures:
            iterators: Tuple[Optional[Closable], ...]
            futures: Tuple["Future[T]", ...]
            iterators, futures = zip(*self._iterators_and_futures)
            self._cancel()
            self._iterators_and_futures.clear()
            wait(futures)
            del futures
            for iterator in iterators:
                if iterator is not None:
                    iterator.close()
        self._shutdown_executor()

    def _cancel(self) -> None:
        for _, future in self._iterators_and_futures:
            future.cancel()
