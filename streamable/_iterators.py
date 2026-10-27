import datetime
import queue
import sys
import time
import weakref
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from concurrent.futures import (
    FIRST_COMPLETED,
    Executor,
    Future,
    ThreadPoolExecutor,
    wait,
)
from threading import Event, Semaphore, Thread
from typing import (
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
from streamable._tools._observation import Observation
from streamable._tools._sentinel import STOP_ITERATION
from streamable._tools._validation import validate_sync_flatten_iterable

T = TypeVar("T")
U = TypeVar("U")
Exc = TypeVar("Exc", bound=Exception)


class _RaisingIterator(Iterator[T]):
    __slots__ = ("upstream",)

    def __init__(
        self,
        upstream: Iterator[Union[T, ExceptionContainer]],
    ) -> None:
        self.upstream = upstream

    def __next__(self) -> T:
        elem = self.upstream.__next__()
        if isinstance(elem, ExceptionContainer):
            try:
                raise elem.exception
            finally:
                del elem
        return elem


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


def _pull_into(iterator: Iterator[T], future: "Future[T]") -> None:
    try:
        future.set_result(iterator.__next__())
    except BaseException as e:
        future.set_exception(e)
    finally:
        # avoid a reference cycle: exception -> traceback -> this frame -> future
        del future


def pull_in_background(iterator: Iterator[T]) -> "Future[T]":
    """Pulls the next element of `iterator` in a daemon thread."""
    future: "Future[T]" = Future()
    Thread(target=_pull_into, args=(iterator, future), daemon=True).start()
    return future


##########
# buffer #
##########


class BufferIterator(_RaisingIterator[T]):
    __slots__ = ("upstream", "up_to", "buffer")

    def __init__(
        self,
        upstream: Iterator[T],
        up_to: Optional[int],
    ) -> None:
        self.upstream = upstream
        self.up_to = up_to or sys.maxsize
        self.buffer: "queue.Queue[Union[T, ExceptionContainer]]" = queue.Queue()
        super().__init__(
            BufferIterator.create_generator(self.upstream, self.up_to, self.buffer)
        )

    @staticmethod
    def create_generator(
        upstream: Iterator[T],
        up_to: int,
        buffer: "queue.Queue[Union[T, ExceptionContainer]]",
    ) -> Generator[Union[T, ExceptionContainer], None, None]:
        limit = Semaphore(up_to)
        stopped = Event()
        base_exception: Deque[BaseException] = deque()
        thread = Thread(
            target=puller_task,
            args=(upstream, buffer, limit, stopped, base_exception),
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
            buffer.queue.clear()


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

    def _timeout(self, groups: Dict[U, Tuple[float, List[T]]]) -> Optional[float]:
        if groups:
            oldest_group_time = next(iter(groups.values()))[0]
            timeout = oldest_group_time + self.within_seconds - time.perf_counter()
            return max(0, timeout)
        return None

    def __iter__(self) -> Iterator[Union[ExceptionContainer, Tuple[U, List[T]]]]:
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
                        if isinstance(elem, ExceptionContainer):
                            try:
                                raise elem.exception
                            finally:
                                del elem
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
            next_elem.queue.clear()


class GroupByWithinIterator(_RaisingIterator[Tuple[U, List[T]]]):
    __slots__ = ()

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
        "_activated",
        "_active",
        "_start_point",
        "_do_base_exception",
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
        self._activated = False
        self._active = False
        self._start_point: datetime.datetime
        self._do_base_exception: Optional[BaseException] = None

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
            self._active = False
            try:
                raise self._do_base_exception
            finally:
                self._do_base_exception = None

    @abstractmethod
    def _threshold(self, observed: int) -> int: ...

    def __next__(self) -> T:
        if not self._activated:
            self._activate()
        if not self._active:
            raise StopIteration
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
            return self.__next__()
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
            if self._do_base_exception:
                return
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

    def _launch_next(
        self,
        future_results: "FutureResults[U]",
        context: Executor,
        future_upstream_elem: "Optional[Future[T]]" = None,
    ) -> bool:
        try:
            if future_upstream_elem:
                elem = future_upstream_elem.result()
            else:
                elem = self.upstream.__next__()
            future = self._launch_task(elem, context)
        except StopIteration:
            return False
        except Exception as e:
            future = FailedFuture(e)
        future_results.add(future)
        del future
        return True

    def __iter__(self) -> Iterator[Union[U, ExceptionContainer]]:
        future_results: FutureResults[U] = (
            FDFOFutureResults() if self.as_completed else FIFOFutureResults()
        )
        with self._task_context() as executor:
            try:
                if isinstance(self.upstream, BufferIterator):
                    upstream_exhausted = False
                    # at most one pull of the upstream in the background, consumed exactly once
                    pending_pull: Optional["Future[T]"] = None
                    try:
                        while not upstream_exhausted or future_results:
                            # launch the available upstream elements, up to `concurrency`
                            while (
                                not upstream_exhausted
                                and len(future_results) < self.concurrency
                            ):
                                pull: Optional["Future[T]"] = None
                                if pending_pull:
                                    if future_results and not pending_pull.done():
                                        break
                                    pull, pending_pull = pending_pull, None
                                elif (
                                    future_results and not self.upstream.buffer.qsize()
                                ):
                                    break
                                # blocks only if nothing is in flight
                                if not self._launch_next(
                                    future_results, executor, pull
                                ):
                                    upstream_exhausted = True
                                del pull
                            if not future_results:
                                continue
                            waitables = future_results.waitables()
                            if (
                                not upstream_exhausted
                                and len(future_results) < self.concurrency
                            ):
                                # wait for the next result or the next upstream element, whichever comes first
                                if not pending_pull:
                                    pending_pull = pull_in_background(self.upstream)
                                to_wait: "List[Future]" = [*waitables, pending_pull]
                                wait(to_wait, return_when=FIRST_COMPLETED)
                                del to_wait
                                if not any(future.done() for future in waitables):
                                    del waitables
                                    continue
                            del waitables
                            yield future_results.__next__()
                    finally:
                        # like the buffer's own puller: wait for the in-flight pull
                        if pending_pull:
                            wait([pending_pull])
                            del pending_pull

                else:
                    # queue tasks up to buffersize
                    while len(future_results) < self.concurrency:
                        if not self._launch_next(future_results, executor):
                            # no more tasks to queue
                            break
                    # queue, wait, yield
                    while future_results:
                        self._launch_next(future_results, executor)
                        yield future_results.__next__()
            finally:
                future_results.cancel()
                future_results.clear()


class ConcurrentMapIterator(_RaisingIterator[U]):
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
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            iterator_and_future_pairs: Deque[
                Tuple[
                    Optional[Iterator[T]],
                    "Future[T]",
                ]
            ] = deque()
            to_yield: Deque[Union[T, ExceptionContainer]] = deque(maxlen=1)
            iterator_to_queue: Optional[Iterator[T]] = None
            # wait, queue, yield (FIFO)
            while True:
                if iterator_and_future_pairs:
                    iterator, future = iterator_and_future_pairs.popleft()
                    try:
                        elem = ExceptionContainer.result(future)
                    finally:
                        del future
                    if elem is not STOP_ITERATION:
                        to_yield.append(elem)
                        iterator_to_queue = iterator
                    del elem

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
                            iterator_and_future_pairs.append(
                                (iterator_to_queue, FailedFuture(e))
                            )
                            continue
                    future = executor.submit(next, iterator_to_queue, STOP_ITERATION)
                    iterator_and_future_pairs.append((iterator_to_queue, future))
                    iterator_to_queue = None
                if to_yield:
                    yield to_yield.pop()
                if not iterator_and_future_pairs:
                    break


class ConcurrentFlattenIterator(_RaisingIterator[T]):
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
