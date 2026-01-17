import asyncio
import datetime
import sys
from threading import Thread
import time
from asyncio import Task
from abc import ABC, abstractmethod
from collections import defaultdict, deque

from concurrent.futures import Executor, Future, ThreadPoolExecutor
from contextlib import suppress
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    AsyncIterator,
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

if TYPE_CHECKING:
    from streamable._stream import stream
from streamable._tools._async import (
    AsyncFunction,
    LoopClosingMixin,
)
from streamable._tools._contextmanager import noop_context_manager
from streamable._tools._error import ExceptionContainer, RaisingIterator

from streamable._tools._future import (
    AsyncFDFOFutureResultCollection,
    ExecutorFDFOFutureResultCollection,
    AsyncFIFOFutureResultCollection,
    ExecutorFIFOFutureResultCollection,
    FutureResult,
    FutureResultCollection,
)

if sys.version_info < (3, 10):  # pragma: no cover
    from streamable._tools._async import anext

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
    __slots__ = ("iterator", "loop_getter", "_current_iterator_elem")

    def __init__(
        self,
        loop_getter: Callable[[], asyncio.AbstractEventLoop],
        iterator: Iterator[Union[Iterable[T], AsyncIterable[T]]],
    ) -> None:
        self.iterator = iterator
        self.loop_getter = loop_getter
        self._current_iterator_elem: Union[Iterator[T], AsyncIterator[T]] = (
            tuple().__iter__()
        )

    def __next__(self) -> T:
        while True:
            try:
                if isinstance(self._current_iterator_elem, Iterator):
                    return self._current_iterator_elem.__next__()
                else:
                    return self.loop_getter().run_until_complete(
                        self._current_iterator_elem.__anext__()
                    )
            except (StopIteration, StopAsyncIteration):
                iterable = self.iterator.__next__()
                if isinstance(iterable, Iterable):
                    self._current_iterator_elem = iterable.__iter__()
                else:
                    self._current_iterator_elem = iterable.__aiter__()


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
        "_to_raise",
        "_start_point",
        "_just_raised",
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
        self._to_raise: Optional[Exception] = None
        self._just_raised = False
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
        try:
            self.do(self._observation())
        except Exception as e:
            self._to_raise = e

    def _raise_next(self) -> None:
        if self._to_raise and not self._just_raised:
            self._just_raised = True
            try:
                raise self._to_raise
            finally:
                self._to_raise = None
        self._just_raised = False

    @abstractmethod
    def _threshold(self, observed: int) -> int: ...

    def __next__(self) -> T:
        if not self._active:
            self._activate()
        self._raise_next()
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
    __slots__ = ("every",)

    def __init__(
        self,
        iterator: Iterator[T],
        subject: str,
        every: datetime.timedelta,
        do: Callable[["stream.Observation"], Any],
    ) -> None:
        super().__init__(iterator, subject, do)
        self.every = every

    def _observer(self) -> None:
        every_seconds = self.every.total_seconds()
        while self._active:
            self._observe()
            time.sleep(every_seconds)

    def _activate(self) -> None:
        super()._activate()
        Thread(target=self._observer, daemon=True).start()

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


class BufferIterable(Iterable[Union[T, ExceptionContainer]]):
    __slots__ = ("iterator", "up_to", "_buffer", "_next")

    def __init__(
        self,
        iterator: Iterator[T],
        up_to: int,
    ) -> None:
        self.iterator = iterator
        self.up_to = up_to
        self._buffer: Deque[Future[Union[T, ExceptionContainer]]] = deque(
            maxlen=up_to + 1
        )
        self._next = ExceptionContainer.wrap(next)

    def __iter__(self) -> Iterator[Union[T, ExceptionContainer]]:
        with ThreadPoolExecutor(max_workers=1) as executor:
            while True:
                while len(self._buffer) <= self.up_to:
                    self._buffer.append(executor.submit(self._next, self.iterator))
                yield self._buffer.popleft().result()


class BufferIterator(RaisingIterator[T]):
    __slots__ = ()

    def __init__(
        self,
        iterator: Iterator[T],
        up_to: int,
    ) -> None:
        super().__init__(BufferIterable(iterator, up_to).__iter__())


##################
# concurrent map #
##################


class _BaseConcurrentMapIterable(
    Generic[T, U], ABC, Iterable[Union[U, ExceptionContainer]]
):
    __slots__ = ("iterator", "concurrency", "as_completed", "_context_manager")

    def __init__(
        self,
        iterator: Iterator[T],
        concurrency: int,
        as_completed: bool,
        context_manager: Optional[ContextManager] = None,
    ) -> None:
        self.iterator = iterator
        self.concurrency = concurrency
        self.as_completed = as_completed
        self._context_manager = context_manager or noop_context_manager()

    @abstractmethod
    def _launch_task(self, elem: T) -> "Future[Union[U, ExceptionContainer]]": ...

    @abstractmethod
    def _future_result_collection(
        self,
    ) -> FutureResultCollection[Union[U, ExceptionContainer]]: ...

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
            future_results = self._future_result_collection()

            # queue tasks up to buffersize
            while len(future_results) < self.concurrency:
                future = self._next_future()
                if not future:
                    # no more tasks to queue
                    break
                future_results.add(future)

            # queue, wait, yield
            while future_results:
                future = self._next_future()
                if future:
                    future_results.add(future)
                yield future_results.__next__()


class _ExecutorConcurrentMapIterable(_BaseConcurrentMapIterable[T, U]):
    __slots__ = ("into", "executor")

    def __init__(
        self,
        iterator: Iterator[T],
        into: Callable[[T], U],
        concurrency: Union[int, Executor],
        as_completed: bool,
    ) -> None:
        self.into = ExceptionContainer.wrap(into)
        if isinstance(concurrency, int):
            self.executor: Executor = ThreadPoolExecutor(max_workers=concurrency)
            super().__init__(
                iterator, concurrency, as_completed, context_manager=self.executor
            )
        else:
            self.executor = concurrency
            super().__init__(
                iterator, getattr(self.executor, "_max_workers"), as_completed
            )

    def _launch_task(self, elem: T) -> "Future[Union[U, ExceptionContainer]]":
        return self.executor.submit(self.into, elem)

    def _future_result_collection(
        self,
    ) -> FutureResultCollection[Union[U, ExceptionContainer]]:
        if self.as_completed:
            return ExecutorFDFOFutureResultCollection()
        return ExecutorFIFOFutureResultCollection()


class ExecutorConcurrentMapIterator(RaisingIterator[U]):
    __slots__ = ()

    def __init__(
        self,
        iterator: Iterator[T],
        into: Callable[[T], U],
        concurrency: Union[int, Executor],
        as_completed: bool,
    ) -> None:
        super().__init__(
            _ExecutorConcurrentMapIterable(
                iterator,
                into,
                concurrency,
                as_completed,
            ).__iter__()
        )


class _AsyncConcurrentMapIterable(_BaseConcurrentMapIterable[T, U], LoopClosingMixin):
    __slots__ = (
        "iterator",
        "concurrency",
        "as_completed",
        "_context_manager",
        "loop",
        "into",
        "__semaphore",
    )

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        iterator: Iterator[T],
        into: AsyncFunction[T, U],
        concurrency: int,
        as_completed: bool,
    ) -> None:
        super().__init__(iterator, concurrency, as_completed)
        self.into = ExceptionContainer.awrap(into)
        self.loop = loop
        self.__semaphore: Optional[asyncio.Semaphore] = None

    @property
    def _semaphore(self) -> asyncio.Semaphore:
        if not self.__semaphore:
            self.__semaphore = asyncio.Semaphore(self.concurrency)
        return self.__semaphore

    async def _semaphored(self, elem: T) -> Union[U, ExceptionContainer]:
        async with self._semaphore:
            return await self.into(elem)

    def _launch_task(self, elem: T) -> "Future[Union[U, ExceptionContainer]]":
        return cast(
            "Future[Union[U, ExceptionContainer]]",
            self.loop.create_task(self._semaphored(elem)),
        )

    def _future_result_collection(
        self,
    ) -> FutureResultCollection[Union[U, ExceptionContainer]]:
        if self.as_completed:
            return AsyncFDFOFutureResultCollection(self.loop)
        return AsyncFIFOFutureResultCollection(self.loop)


class AsyncConcurrentMapIterator(RaisingIterator[U]):
    __slots__ = ()

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        iterator: Iterator[T],
        into: AsyncFunction[T, U],
        concurrency: int,
        as_completed: bool,
    ) -> None:
        super().__init__(
            _AsyncConcurrentMapIterable(
                loop,
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
        "loop_getter",
        "iterables_iterator",
        "concurrency",
        "_next",
        "_anext",
        "_executor",
    )

    def __init__(
        self,
        loop_getter: Callable[[], asyncio.AbstractEventLoop],
        iterables_iterator: Iterator[Union[Iterable[T], AsyncIterable[T]]],
        concurrency: int,
    ) -> None:
        self.loop_getter = loop_getter
        self.iterables_iterator = iterables_iterator
        self.concurrency = concurrency
        self._next = ExceptionContainer.wrap(next)
        self._anext = ExceptionContainer.awrap(anext)
        self._executor: Optional[Executor] = None

    @property
    def executor(self) -> Executor:
        if not self._executor:
            self._executor = ThreadPoolExecutor(max_workers=self.concurrency)
        return self._executor

    def _get_result(
        self,
        future: "Union[Future[Union[T, ExceptionContainer]], Task[Union[T, ExceptionContainer]]]",
    ) -> Union[T, ExceptionContainer]:
        if isinstance(future, Future):
            return future.result()
        else:
            return self.loop_getter().run_until_complete(future)

    def __iter__(self) -> Iterator[Union[T, ExceptionContainer]]:
        iterator_and_future_pairs: Deque[
            Tuple[
                Union[None, Iterator[T], AsyncIterator[T]],
                "Union[Future[Union[T, ExceptionContainer]], Task[Union[T, ExceptionContainer]]]",
            ]
        ] = deque()
        to_yield: Deque[Union[T, ExceptionContainer]] = deque(maxlen=1)
        iterator_to_queue: Union[None, Iterator[T], AsyncIterator[T]] = None
        # wait, queue, yield (FIFO)
        while True:
            if iterator_and_future_pairs:
                iterator, future = iterator_and_future_pairs.popleft()
                elem = self._get_result(future)
                if not isinstance(elem, ExceptionContainer) or not isinstance(
                    elem.exception, (StopIteration, StopAsyncIteration)
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
                        if isinstance(iterable, Iterable):
                            iterator_to_queue = iterable.__iter__()
                        else:
                            iterator_to_queue = iterable.__aiter__()
                    except Exception as e:
                        iterator_to_queue = None
                        future = FutureResult(ExceptionContainer(e))
                        iterator_and_future_pairs.append((iterator_to_queue, future))
                        continue
                if isinstance(iterator_to_queue, Iterator):
                    future = self.executor.submit(self._next, iterator_to_queue)
                else:
                    future = self.loop_getter().create_task(
                        self._anext(iterator_to_queue)
                    )
                iterator_and_future_pairs.append((iterator_to_queue, future))
                iterator_to_queue = None
            if to_yield:
                yield to_yield.pop()
            if not iterator_and_future_pairs:
                break
        if self._executor:
            self._executor.shutdown()
            self._executor = None


class ConcurrentFlattenIterator(RaisingIterator[T]):
    __slots__ = ()

    def __init__(
        self,
        loop_getter: Callable[[], asyncio.AbstractEventLoop],
        iterables_iterator: Iterator[Union[Iterable[T], AsyncIterable[T]]],
        concurrency: int,
    ) -> None:
        super().__init__(
            _ConcurrentFlattenIterable(
                loop_getter,
                iterables_iterator,
                concurrency,
            ).__iter__()
        )
