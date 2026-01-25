import asyncio
from asyncio.futures import Future
import datetime
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from concurrent.futures import Executor, ThreadPoolExecutor
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
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

from streamable._tools._sentinel import STOP_ITERATION
from streamable._tools._validation import validate_async_flatten_iterable

if TYPE_CHECKING:
    from streamable._stream import stream
from streamable._tools._afuture import (
    FutureResult,
    FDFOFutureResults,
    FIFOFutureResults,
    FutureResults,
)
from streamable._tools._async import AsyncFunction, empty_aiter
from streamable._tools._contextmanager import noop_context_manager
from streamable._tools._error import ExceptionContainer, RaisingAsyncIterator

from streamable._tools._async import anext

T = TypeVar("T")
U = TypeVar("U")


#########
# catch #
#########


class CatchAsyncIterator(AsyncIterator[Union[T, U]]):
    __slots__ = ("iterator", "errors", "where", "replace", "do", "stop", "_stopped")

    def __init__(
        self,
        iterator: AsyncIterator[T],
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        where: Optional[AsyncFunction[Exception, Any]],
        replace: Optional[AsyncFunction[Exception, U]],
        do: Optional[AsyncFunction[Exception, Any]],
        stop: bool,
    ) -> None:
        self.iterator = iterator
        self.errors = errors
        self.where = where
        self.replace = replace
        self.do = do
        self.stop = stop
        self._stopped = False

    async def __anext__(self) -> Union[T, U]:
        while True:
            if self._stopped:
                raise StopAsyncIteration
            try:
                return await self.iterator.__anext__()
            except StopAsyncIteration:
                raise
            except self.errors as e:
                if not self.where or await self.where(e):
                    if self.stop:
                        self._stopped = True
                    if self.do:
                        await self.do(e)
                    if self.replace:
                        return await self.replace(e)
                    continue
                raise


###########
# flatten #
###########


class FlattenAsyncIterator(AsyncIterator[T]):
    __slots__ = ("iterator", "_current_iterator_elem")

    def __init__(
        self, iterator: AsyncIterator[Union[Iterable[T], AsyncIterable[T]]]
    ) -> None:
        self.iterator = iterator
        self._current_iterator_elem: Union[Iterator[T], AsyncIterator[T]] = (
            empty_aiter()
        )

    async def __anext__(self) -> T:
        while True:
            try:
                if isinstance(self._current_iterator_elem, AsyncIterator):
                    return await self._current_iterator_elem.__anext__()
                else:
                    return self._current_iterator_elem.__next__()
            except (StopIteration, StopAsyncIteration):
                iterable = await self.iterator.__anext__()
                validate_async_flatten_iterable(iterable)
                if isinstance(iterable, AsyncIterable):
                    self._current_iterator_elem = iterable.__aiter__()
                else:
                    self._current_iterator_elem = iterable.__iter__()


#########
# group #
#########


class _BaseGroupAsyncIterator(Generic[T]):
    __slots__ = ("iterator", "up_to", "_every_seconds", "_to_raise", "_last_yield_at")

    def __init__(
        self,
        iterator: AsyncIterator[T],
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


class GroupAsyncIterator(_BaseGroupAsyncIterator[T], AsyncIterator[List[T]]):
    __slots__ = ("_current_group",)

    def __init__(
        self,
        iterator: AsyncIterator[T],
        up_to: Optional[int],
        every: Optional[datetime.timedelta],
    ) -> None:
        super().__init__(iterator, up_to, every)
        self._current_group: List[T] = []

    async def __anext__(self) -> List[T]:
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
                self._current_group.append(await self.iterator.__anext__())
        except Exception as e:
            if not self._current_group:
                raise
            self._to_raise = e

        group, self._current_group = self._current_group, []
        self._remember_group_time()
        return group


class GroupbyAsyncIterator(
    _BaseGroupAsyncIterator[T], AsyncIterator[Tuple[U, List[T]]]
):
    __slots__ = ("by", "_is_exhausted", "_current_groups")

    def __init__(
        self,
        iterator: AsyncIterator[T],
        by: AsyncFunction[T, U],
        up_to: Optional[int],
        every: Optional[datetime.timedelta],
    ) -> None:
        super().__init__(iterator, up_to, every)
        self.by = by
        self._is_exhausted = False
        self._current_groups: DefaultDict[U, List[T]] = defaultdict(list)

    async def _group_next_elem(self) -> None:
        elem = await self.iterator.__anext__()
        self._current_groups[await self.by(elem)].append(elem)

    def _pop_full_group(self) -> Optional[Tuple[U, List[T]]]:
        for key, group in self._current_groups.items():
            if len(group) >= self.up_to:
                return key, self._current_groups.pop(key)
        return None

    def _pop_oldest_group(self) -> Tuple[U, List[T]]:
        first_key: U = self._current_groups.__iter__().__next__()
        return first_key, self._current_groups.pop(first_key)

    async def __anext__(self) -> Tuple[U, List[T]]:
        self._init_last_group_time()
        if self._is_exhausted:
            if self._current_groups:
                return self._pop_oldest_group()
            raise StopAsyncIteration

        if self._to_raise:
            if self._current_groups:
                self._remember_group_time()
                return self._pop_oldest_group()
            try:
                raise self._to_raise
            finally:
                self._to_raise = None

        try:
            await self._group_next_elem()

            full_group: Optional[Tuple[U, List[T]]] = self._pop_full_group()
            while not full_group and not self._every_seconds_elapsed():
                await self._group_next_elem()
                full_group = self._pop_full_group()

            self._remember_group_time()
            return full_group or self._pop_oldest_group()

        except StopAsyncIteration:
            self._is_exhausted = True
            return await self.__anext__()

        except Exception as e:
            self._to_raise = e
            return await self.__anext__()


########
# skip #
########


class CountSkipAsyncIterator(AsyncIterator[T]):
    __slots__ = ("iterator", "_remaining_to_skip")

    def __init__(self, iterator: AsyncIterator[T], count: int) -> None:
        self.iterator = iterator
        self._remaining_to_skip = count

    async def __anext__(self) -> T:
        while self._remaining_to_skip > 0:
            await self.iterator.__anext__()
            # do not count exceptions as skipped elements
            self._remaining_to_skip -= 1
        return await self.iterator.__anext__()


class PredicateSkipAsyncIterator(AsyncIterator[T]):
    __slots__ = ("iterator", "until", "_satisfied")

    def __init__(
        self, iterator: AsyncIterator[T], until: AsyncFunction[T, Any]
    ) -> None:
        self.iterator = iterator
        self.until = until
        self._satisfied = False

    async def __anext__(self) -> T:
        elem = await self.iterator.__anext__()
        if not self._satisfied:
            while not await self.until(elem):
                elem = await self.iterator.__anext__()
            self._satisfied = True
        return elem


############
# take #
############


class CountTakeAsyncIterator(AsyncIterator[T]):
    __slots__ = ("iterator", "_remaining_to_take")

    def __init__(self, iterator: AsyncIterator[T], count: int) -> None:
        self.iterator = iterator
        self._remaining_to_take = count

    async def __anext__(self) -> T:
        if self._remaining_to_take <= 0:
            raise StopAsyncIteration
        elem = await self.iterator.__anext__()
        self._remaining_to_take -= 1
        return elem


class PredicateTakeAsyncIterator(AsyncIterator[T]):
    __slots__ = ("iterator", "until", "_satisfied")

    def __init__(
        self, iterator: AsyncIterator[T], until: AsyncFunction[T, Any]
    ) -> None:
        self.iterator = iterator
        self.until = until
        self._satisfied = False

    async def __anext__(self) -> T:
        if self._satisfied:
            raise StopAsyncIteration
        elem = await self.iterator.__anext__()
        if await self.until(elem):
            self._satisfied = True
            raise StopAsyncIteration
        return elem


#######
# map #
#######


class MapAsyncIterator(AsyncIterator[U]):
    __slots__ = ("iterator", "to")

    def __init__(
        self,
        iterator: AsyncIterator[T],
        into: AsyncFunction[T, U],
    ) -> None:
        self.iterator = iterator
        self.to = into

    async def __anext__(self) -> U:
        return await self.to(await self.iterator.__anext__())


##########
# filter #
##########


class FilterAsyncIterator(AsyncIterator[T]):
    __slots__ = ("iterator", "where")

    def __init__(
        self,
        iterator: AsyncIterator[T],
        where: AsyncFunction[T, Any],
    ) -> None:
        self.iterator = iterator
        self.where = where

    async def __anext__(self) -> T:
        while True:
            elem = await self.iterator.__anext__()
            if await self.where(elem):
                return elem


###########
# observe #
###########


class _BaseObserveAsyncIterator(AsyncIterator[T]):
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
        "_just_raised",
        "_start_point",
    )

    def __init__(
        self,
        iterator: AsyncIterator[T],
        subject: str,
        do: AsyncFunction["stream.Observation", Any],
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

    async def _activate(self) -> None:
        self._start_point = self._time_point()
        self._active = True

    async def _observe(self) -> None:
        self._emissions_observed = self._emissions
        try:
            await self.do(self._observation())
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

    async def __anext__(self) -> T:
        if not self._active:
            await self._activate()
        self._raise_next()
        try:
            elem = await self.iterator.__anext__()
            self._elements += 1
            if self._elements >= self._threshold(self._elements_observed):
                await self._observe()
                self._elements_observed = self._elements
            return elem
        except StopAsyncIteration:
            if not self._emissions or self._emissions > self._emissions_observed:
                await self._observe()
            self._active = False
            raise
        except Exception:
            self._errors += 1
            if self._errors >= self._threshold(self._errors_observed):
                await self._observe()
                self._errors_observed = self._errors
            raise


class PowerObserveAsyncIterator(_BaseObserveAsyncIterator[T]):
    __slots__ = ("base",)

    def __init__(
        self,
        iterator: AsyncIterator[T],
        subject: str,
        do: AsyncFunction["stream.Observation", Any],
        base: int = 2,
    ) -> None:
        super().__init__(iterator, subject, do)
        self.base = base

    def _threshold(self, observed: int) -> int:
        return self.base * observed


class EveryIntObserveAsyncIterator(_BaseObserveAsyncIterator[T]):
    __slots__ = ("every",)

    def __init__(
        self,
        iterator: AsyncIterator[T],
        subject: str,
        every: int,
        do: AsyncFunction["stream.Observation", Any],
    ) -> None:
        super().__init__(iterator, subject, do)
        self.every = every

    def _threshold(self, observed: int) -> int:
        if not observed:
            return 0
        if observed == 1:
            return self.every
        return observed + self.every


class EveryIntervalObserveAsyncIterator(_BaseObserveAsyncIterator[T]):
    __slots__ = ("__weakref__", "every")

    def __init__(
        self,
        iterator: AsyncIterator[T],
        subject: str,
        every: datetime.timedelta,
        do: AsyncFunction["stream.Observation", Any],
    ) -> None:
        super().__init__(iterator, subject, do)
        self.every = every

    @staticmethod
    async def _observer(
        weak_self: "weakref.ReferenceType[EveryIntervalObserveAsyncIterator[T]]",
        every_seconds: float,
    ) -> None:
        self = weak_self()
        while self and self._active:
            await self._observe()
            self = None
            await asyncio.sleep(every_seconds)
            self = weak_self()

    async def _activate(self) -> None:
        await super()._activate()
        asyncio.create_task(
            self._observer(weakref.ref(self), self.every.total_seconds())
        )
        await asyncio.sleep(0)

    def _threshold(self, observed: int) -> int:
        return cast(int, float("inf"))


############
# throttle #
############


class ThrottleAsyncIterator(AsyncIterator[T]):
    __slots__ = ("iterator", "up_to", "_window_seconds", "_emission_timestamps")

    def __init__(
        self,
        iterator: AsyncIterator[T],
        up_to: int,
        per: datetime.timedelta,
    ) -> None:
        self.iterator = iterator
        self.up_to = up_to
        self._window_seconds = per.total_seconds()
        self._emission_timestamps: Deque[float] = deque()

    async def __anext__(self) -> T:
        elem: Optional[T] = None
        error: Optional[Exception] = None
        try:
            elem = await self.iterator.__anext__()
        except StopAsyncIteration:
            raise
        except Exception as e:
            error = e

        # did we reach `up_to` emissions?
        if len(self._emission_timestamps) >= self.up_to:
            # sleep until the oldest emission leaves the window
            oldest_leaves_window_at = (
                self._emission_timestamps[0] + self._window_seconds
            )
            await asyncio.sleep(max(0, oldest_leaves_window_at - time.perf_counter()))
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


class _BufferAsyncIterable(AsyncIterable[Union[T, ExceptionContainer]]):
    __slots__ = ("iterator", "up_to", "_buffer", "_slots", "_stopped")

    def __init__(
        self,
        iterator: AsyncIterator[T],
        up_to: int,
    ) -> None:
        self.iterator = iterator
        self.up_to = up_to
        self._buffer: "Optional[asyncio.Queue[Union[T, ExceptionContainer]]]" = None
        self._slots: Optional[asyncio.Semaphore] = None
        self._stopped = False

    @property
    def _lazy_buffer(self) -> "asyncio.Queue[Union[T, ExceptionContainer]]":
        if not self._buffer:
            self._buffer = asyncio.Queue()
        return self._buffer

    @property
    def _lazy_slots(self) -> asyncio.Semaphore:
        if not self._slots:
            self._slots = asyncio.Semaphore(self.up_to)
        return self._slots

    async def _buffer_upstream(self) -> None:
        elem: Union[T, ExceptionContainer]
        await self._lazy_slots.acquire()
        while not self._stopped:
            try:
                elem = await self.iterator.__anext__()
            except StopAsyncIteration:
                elem = STOP_ITERATION
                self._stopped = True
            except Exception as e:
                elem = ExceptionContainer(e)
            self._lazy_buffer.put_nowait(elem)
            await self._lazy_slots.acquire()

    async def __aiter__(self) -> AsyncIterator[Union[T, ExceptionContainer]]:
        task = asyncio.create_task(self._buffer_upstream())
        try:
            while True:
                elem = await self._lazy_buffer.get()
                if elem is STOP_ITERATION:
                    break
                self._lazy_slots.release()
                yield elem
        finally:
            self._stopped = True
            self._lazy_slots.release()
            await task


class BufferAsyncIterator(RaisingAsyncIterator[T]):
    __slots__ = ()

    def __init__(
        self,
        iterator: AsyncIterator[T],
        up_to: int,
    ) -> None:
        super().__init__(_BufferAsyncIterable(iterator, up_to).__aiter__())


##################
# concurrent map #
##################


class _BaseConcurrentMapAsyncIterable(
    Generic[T, U],
    ABC,
    AsyncIterable[Union[U, ExceptionContainer]],
):
    __slots__ = ("iterator", "concurrency", "_context_manager", "_future_results")

    def __init__(
        self,
        iterator: AsyncIterator[T],
        concurrency: int,
        as_completed: bool,
        context_manager: Optional[ContextManager] = None,
    ) -> None:
        self.iterator = iterator
        self.concurrency = concurrency
        self._context_manager = context_manager or noop_context_manager()
        self._future_results: FutureResults[Union[U, ExceptionContainer]] = (
            FDFOFutureResults() if as_completed else FIFOFutureResults()
        )

    @abstractmethod
    def _launch_task(self, elem: T) -> "Future[Union[U, ExceptionContainer]]": ...

    async def _next_future(
        self,
    ) -> Optional["Future[Union[U, ExceptionContainer]]"]:
        try:
            elem = await self.iterator.__anext__()
        except StopAsyncIteration:
            return None
        except Exception as e:
            return FutureResult(ExceptionContainer(e))
        return self._launch_task(elem)

    async def __aiter__(
        self,
    ) -> AsyncIterator[Union[U, ExceptionContainer]]:
        with self._context_manager:
            # queue tasks up to buffersize
            while len(self._future_results) < self.concurrency:
                future = await self._next_future()
                if not future:
                    # no more tasks to queue
                    break
                self._future_results.add(future)

            # queue, wait, yield
            while self._future_results:
                future = await self._next_future()
                if future:
                    self._future_results.add(future)
                yield await self._future_results.__anext__()


class _AsyncConcurrentMapAsyncIterable(_BaseConcurrentMapAsyncIterable[T, U]):
    __slots__ = ("into", "_semaphore")

    def __init__(
        self,
        iterator: AsyncIterator[T],
        into: AsyncFunction[T, U],
        concurrency: int,
        as_completed: bool,
    ) -> None:
        super().__init__(iterator, concurrency, as_completed)
        self.into = ExceptionContainer.awrap(into)
        self._semaphore: Optional[asyncio.Semaphore] = None

    async def _semaphored(self, elem: T) -> Union[U, ExceptionContainer]:
        self._semaphore = self._semaphore or asyncio.Semaphore(self.concurrency)
        async with self._semaphore:
            return await self.into(elem)

    def _launch_task(self, elem: T) -> "Future[Union[U, ExceptionContainer]]":
        return asyncio.create_task(self._semaphored(elem))


class AsyncConcurrentMapAsyncIterator(RaisingAsyncIterator[U]):
    __slots__ = ()

    def __init__(
        self,
        iterator: AsyncIterator[T],
        into: AsyncFunction[T, U],
        concurrency: int,
        as_completed: bool,
    ) -> None:
        super().__init__(
            _AsyncConcurrentMapAsyncIterable(
                iterator,
                into,
                concurrency,
                as_completed,
            ).__aiter__()
        )


class _ExecutorConcurrentMapAsyncIterable(_BaseConcurrentMapAsyncIterable[T, U]):
    __slots__ = ("into", "_executor")

    def __init__(
        self,
        iterator: AsyncIterator[T],
        into: Callable[[T], U],
        concurrency: Union[int, Executor],
        as_completed: bool,
    ) -> None:
        self.into = ExceptionContainer.wrap(into)
        if isinstance(concurrency, int):
            self._executor: Executor = ThreadPoolExecutor(max_workers=concurrency)
            super().__init__(
                iterator, concurrency, as_completed, context_manager=self._executor
            )
        else:
            self._executor = concurrency
            super().__init__(
                iterator, getattr(self._executor, "_max_workers"), as_completed
            )

    def _launch_task(self, elem: T) -> "Future[Union[U, ExceptionContainer]]":
        return asyncio.get_running_loop().run_in_executor(
            self._executor, self.into, elem
        )


class ExecutorConcurrentMapAsyncIterator(RaisingAsyncIterator[U]):
    __slots__ = ()

    def __init__(
        self,
        iterator: AsyncIterator[T],
        into: Callable[[T], U],
        concurrency: Union[int, Executor],
        as_completed: bool,
    ) -> None:
        super().__init__(
            _ExecutorConcurrentMapAsyncIterable(
                iterator,
                into,
                concurrency,
                as_completed,
            ).__aiter__()
        )


######################
# concurrent flatten #
######################


class _ConcurrentFlattenAsyncIterable(AsyncIterable[Union[T, ExceptionContainer]]):
    __slots__ = (
        "iterables_iterator",
        "concurrency",
        "_next",
        "_anext",
        "_executor",
    )

    def __init__(
        self,
        iterables_iterator: AsyncIterator[Union[Iterable[T], AsyncIterable[T]]],
        concurrency: int,
    ) -> None:
        self.iterables_iterator = iterables_iterator
        self.concurrency = concurrency
        self._next = ExceptionContainer.wrap(next)
        self._anext = ExceptionContainer.awrap(anext)
        self._executor: Optional[Executor] = None

    @property
    def _lazy_executor(self) -> Executor:
        if not self._executor:
            self._executor = ThreadPoolExecutor(max_workers=self.concurrency)
        return self._executor

    async def __aiter__(
        self,
    ) -> AsyncIterator[Union[T, ExceptionContainer]]:
        iterator_and_future_pairs: Deque[
            Tuple[
                Union[None, Iterator[T], AsyncIterator[T]],
                Awaitable[Union[T, ExceptionContainer]],
            ]
        ] = deque()
        to_yield: Deque[Union[T, ExceptionContainer]] = deque(maxlen=1)
        iterator_to_queue: Union[None, Iterator[T], AsyncIterator[T]] = None
        # wait, queue, yield (FIFO)
        try:
            while True:
                if iterator_and_future_pairs:
                    iterator, future = iterator_and_future_pairs.popleft()
                    elem = await future
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
                                iterable = await self.iterables_iterator.__anext__()
                            except StopAsyncIteration:
                                break
                            validate_async_flatten_iterable(iterable)
                            if isinstance(iterable, AsyncIterable):
                                iterator_to_queue = iterable.__aiter__()
                            else:
                                iterator_to_queue = iterable.__iter__()
                        except Exception as e:
                            iterator_to_queue = None
                            future = FutureResult(ExceptionContainer(e))
                            iterator_and_future_pairs.append(
                                (iterator_to_queue, future)
                            )
                            continue
                    if isinstance(iterator_to_queue, AsyncIterator):
                        future = asyncio.create_task(self._anext(iterator_to_queue))
                    else:
                        future = asyncio.get_running_loop().run_in_executor(
                            self._lazy_executor,
                            self._next,
                            iterator_to_queue,
                        )
                    iterator_and_future_pairs.append((iterator_to_queue, future))
                    iterator_to_queue = None
                if to_yield:
                    yield to_yield.pop()
                if not iterator_and_future_pairs:
                    break
        finally:
            if self._executor:
                self._executor.shutdown()


class ConcurrentFlattenAsyncIterator(RaisingAsyncIterator[T]):
    __slots__ = ()

    def __init__(
        self,
        iterables_iterator: AsyncIterator[Union[Iterable[T], AsyncIterable[T]]],
        concurrency: int,
    ) -> None:
        super().__init__(
            _ConcurrentFlattenAsyncIterable(
                iterables_iterator,
                concurrency,
            ).__aiter__()
        )
