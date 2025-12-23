import asyncio
from asyncio.futures import Future
import datetime
import sys
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from concurrent.futures import Executor, ThreadPoolExecutor
from contextlib import suppress
from math import ceil
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
    NamedTuple,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

if TYPE_CHECKING:
    pass
from streamable._tools._afuture import (
    FutureResult,
    FutureResultCollection,
    FDFOFutureResultCollection,
    FIFOFutureResultCollection,
)
from streamable._tools._async import AsyncCallable, empty_aiter
from streamable._tools._contextmanager import noop_context_manager
from streamable._tools._error import ExceptionContainer
from streamable._tools._logging import get_logger, logfmt_str_escape

if sys.version_info < (3, 10):  # pragma: no cover
    from streamable._tools._async import anext
with suppress(ImportError):
    pass

T = TypeVar("T")
U = TypeVar("U")


#########
# catch #
#########


class CatchAsyncIterator(AsyncIterator[Union[T, U]]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        where: Optional[AsyncCallable[Exception, Any]],
        replace: Optional[AsyncCallable[Exception, U]],
        do: Optional[AsyncCallable[Exception, Any]],
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
                if isinstance(iterable, AsyncIterable):
                    self._current_iterator_elem = iterable.__aiter__()
                else:
                    self._current_iterator_elem = iterable.__iter__()


#########
# group #
#########


class _BaseGroupAsyncIterator(Generic[T]):
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
    def __init__(
        self,
        iterator: AsyncIterator[T],
        by: AsyncCallable[T, U],
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
    def __init__(
        self, iterator: AsyncIterator[T], until: AsyncCallable[T, Any]
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
    def __init__(
        self, iterator: AsyncIterator[T], until: AsyncCallable[T, Any]
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
    def __init__(
        self,
        iterator: AsyncIterator[T],
        into: AsyncCallable[T, U],
    ) -> None:
        self.iterator = iterator
        self.to = into

    async def __anext__(self) -> U:
        return await self.to(await self.iterator.__anext__())


##########
# filter #
##########


class FilterAsyncIterator(AsyncIterator[T]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        where: AsyncCallable[T, Any],
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


class ObserveAsyncIterator(AsyncIterator[T]):
    _FORMAT = "stream={subject} elapsed={elapsed} errors={errors} emissions={emissions}"

    class Observation(NamedTuple):
        subject: str
        elapsed: datetime.timedelta
        errors: int
        emissions: int

    def __init__(
        self,
        iterator: AsyncIterator[T],
        subject: str,
        do: Optional[AsyncCallable[Observation, Any]],
    ) -> None:
        self.iterator = iterator
        self.subject = logfmt_str_escape(subject)
        self.do = do or self._log
        self._emissions = 0
        self._errors = 0
        self._nexts_logged = 0
        self._emissions_logged = 0
        self._errors_logged = 0
        self.__start_point: Optional[datetime.datetime] = None

    async def _log(self, observation: Observation) -> None:
        get_logger().info(self._FORMAT.format(**observation._asdict()))

    def _observation(self) -> Observation:
        return self.Observation(
            subject=self.subject,
            elapsed=self._time_point() - self._start_point(),
            errors=self._errors,
            emissions=self._emissions,
        )

    @staticmethod
    def _time_point() -> datetime.datetime:
        return datetime.datetime.fromtimestamp(time.perf_counter())

    def _start_point(self) -> datetime.datetime:
        if not self.__start_point:
            self.__start_point = self._time_point()
        return self.__start_point

    async def _observe(self) -> None:
        await self.do(self._observation())
        self._nexts_logged = self._emissions + self._errors

    @abstractmethod
    def _should_emit_yield_log(self) -> bool: ...

    @abstractmethod
    def _should_emit_error_log(self) -> bool: ...

    async def __anext__(self) -> T:
        self._start_point()
        try:
            elem = await self.iterator.__anext__()
            self._emissions += 1
            if self._should_emit_yield_log():
                await self._observe()
                self._emissions_logged = self._emissions
            return elem
        except StopAsyncIteration:
            if self._emissions + self._errors > self._nexts_logged:
                await self._observe()
            raise
        except Exception:
            self._errors += 1
            if self._should_emit_error_log():
                await self._observe()
                self._errors_logged = self._errors
            raise


class PowerObserveAsyncIterator(ObserveAsyncIterator[T]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        subject: str,
        do: Optional[AsyncCallable[ObserveAsyncIterator.Observation, Any]],
        base: int = 2,
    ) -> None:
        super().__init__(iterator, subject, do)
        self.base = base

    def _should_emit_yield_log(self) -> bool:
        return self._emissions >= self.base * self._emissions_logged

    def _should_emit_error_log(self) -> bool:
        return self._errors >= self.base * self._errors_logged


class EveryIntObserveAsyncIterator(ObserveAsyncIterator[T]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        subject: str,
        every: int,
        do: Optional[AsyncCallable[ObserveAsyncIterator.Observation, Any]],
    ) -> None:
        super().__init__(iterator, subject, do)
        self.every = every

    def _should_emit_yield_log(self) -> bool:
        # always emit first yield
        return not self._emissions_logged or not self._emissions % self.every

    def _should_emit_error_log(self) -> bool:
        # always emit first error
        return not self._errors_logged or not self._errors % self.every


class EveryIntervalObserveAsyncIterator(ObserveAsyncIterator[T]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        subject: str,
        every: datetime.timedelta,
        do: Optional[AsyncCallable[ObserveAsyncIterator.Observation, Any]],
    ) -> None:
        super().__init__(iterator, subject, do)
        self._every_seconds: float = every.total_seconds()
        self._last_log_time: Optional[float] = None

    def _should_emit_log(self) -> bool:
        now = time.perf_counter()
        # always emit first yield/error
        should = self._last_log_time is None or (
            (now - self._last_log_time) >= self._every_seconds
        )
        if should:
            self._last_log_time = now
        return should

    def _should_emit_yield_log(self) -> bool:
        return self._should_emit_log()

    def _should_emit_error_log(self) -> bool:
        return self._should_emit_log()


############
# throttle #
############


class ThrottleAsyncIterator(AsyncIterator[T]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        up_to: int,
        period: datetime.timedelta,
    ) -> None:
        self.iterator = iterator
        self.up_to = up_to
        self._period_seconds = period.total_seconds()
        self._period_index: int = -1
        self._emissions_in_period = 0
        self._offset: Optional[float] = None

    async def __anext__(self) -> T:
        elem: Optional[T] = None
        caught_error: Optional[Exception] = None
        try:
            elem = await self.iterator.__anext__()
        except StopAsyncIteration:
            raise
        except Exception as e:
            caught_error = e

        now = time.perf_counter()
        if not self._offset:
            self._offset = now
        now -= self._offset

        num_periods = now / self._period_seconds
        period_index = int(num_periods)

        if self._period_index != period_index:
            self._period_index = period_index
            self._emissions_in_period = max(0, self._emissions_in_period - self.up_to)

        if self._emissions_in_period >= self.up_to:
            await asyncio.sleep(
                (ceil(num_periods) - num_periods) * self._period_seconds
            )
        self._emissions_in_period += 1

        if caught_error:
            try:
                raise caught_error
            finally:
                caught_error = None
        return cast(T, elem)


class _RaisingAsyncIterator(AsyncIterator[T]):
    def __init__(
        self,
        iterator: AsyncIterator[Union[T, ExceptionContainer]],
    ) -> None:
        self.iterator = iterator

    async def __anext__(self) -> T:
        elem = await self.iterator.__anext__()
        if isinstance(elem, ExceptionContainer):
            try:
                raise elem.exception
            finally:
                del elem
        return elem


##################
# concurrent map #
##################


class _BaseConcurrentMapAsyncIterable(
    Generic[T, U],
    ABC,
    AsyncIterable[Union[U, ExceptionContainer]],
):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        concurrency: int,
        ordered: bool,
        context_manager: Optional[ContextManager] = None,
    ) -> None:
        self.iterator = iterator
        self.concurrency = concurrency
        self.ordered = ordered
        self._context_manager = context_manager or noop_context_manager()

    @abstractmethod
    def _launch_task(self, elem: T) -> "Future[Union[U, ExceptionContainer]]": ...

    def _future_result_collection(
        self,
    ) -> FutureResultCollection[Union[U, ExceptionContainer]]:
        if self.ordered:
            return FIFOFutureResultCollection()
        return FDFOFutureResultCollection()

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
            future_results = self._future_result_collection()

            # queue tasks up to buffersize
            while len(future_results) < self.concurrency:
                future = await self._next_future()
                if not future:
                    # no more tasks to queue
                    break
                future_results.add(future)

            # queue, wait, yield
            while future_results:
                future = await self._next_future()
                if future:
                    future_results.add(future)
                yield await future_results.__anext__()


class _ConcurrentMapAsyncIterable(_BaseConcurrentMapAsyncIterable[T, U]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        into: Callable[[T], U],
        concurrency: Union[int, Executor],
        ordered: bool,
    ) -> None:
        self.to = into
        if isinstance(concurrency, int):
            self.executor: Executor = ThreadPoolExecutor(max_workers=concurrency)
            super().__init__(
                iterator, concurrency, ordered, context_manager=self.executor
            )
        else:
            self.executor = concurrency
            super().__init__(iterator, getattr(self.executor, "_max_workers"), ordered)

    # picklable
    @staticmethod
    def _safe_to(to: Callable[[T], U], elem: T) -> Union[U, ExceptionContainer]:
        try:
            return to(elem)
        except Exception as e:
            return ExceptionContainer(e)

    def _launch_task(self, elem: T) -> "Future[Union[U, ExceptionContainer]]":
        return asyncio.get_running_loop().run_in_executor(
            self.executor, self._safe_to, self.to, elem
        )


class ConcurrentMapAsyncIterator(_RaisingAsyncIterator[U]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        into: Callable[[T], U],
        concurrency: Union[int, Executor],
        ordered: bool,
    ) -> None:
        super().__init__(
            _ConcurrentMapAsyncIterable(
                iterator,
                into,
                concurrency,
                ordered,
            ).__aiter__()
        )


class _ConcurrentAMapAsyncIterable(_BaseConcurrentMapAsyncIterable[T, U]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        into: AsyncCallable[T, U],
        concurrency: int,
        ordered: bool,
    ) -> None:
        super().__init__(iterator, concurrency, ordered)
        self.to = into
        self._semaphore: Optional[asyncio.Semaphore] = None

    @property
    def semaphore(self) -> asyncio.Semaphore:
        if not self._semaphore:
            self._semaphore = asyncio.Semaphore(self.concurrency)
        return self._semaphore

    async def _safe_to(self, elem: T) -> Union[U, ExceptionContainer]:
        try:
            async with self.semaphore:
                return await self.to(elem)
        except Exception as e:
            return ExceptionContainer(e)

    def _launch_task(self, elem: T) -> "Future[Union[U, ExceptionContainer]]":
        return asyncio.get_running_loop().create_task(self._safe_to(elem))


class ConcurrentAMapAsyncIterator(_RaisingAsyncIterator[U]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        into: AsyncCallable[T, U],
        concurrency: int,
        ordered: bool,
    ) -> None:
        super().__init__(
            _ConcurrentAMapAsyncIterable(
                iterator,
                into,
                concurrency,
                ordered,
            ).__aiter__()
        )


######################
# concurrent flatten #
######################


class _ConcurrentFlattenAsyncIterable(AsyncIterable[Union[T, ExceptionContainer]]):
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
    def executor(self) -> Executor:
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
                        if isinstance(iterable, AsyncIterable):
                            iterator_to_queue = iterable.__aiter__()
                        else:
                            iterator_to_queue = iterable.__iter__()
                    except Exception as e:
                        iterator_to_queue = None
                        future = FutureResult(ExceptionContainer(e))
                        iterator_and_future_pairs.append((iterator_to_queue, future))
                        continue
                if isinstance(iterator_to_queue, AsyncIterator):
                    future = asyncio.get_running_loop().create_task(
                        self._anext(iterator_to_queue)
                    )
                else:
                    future = asyncio.get_running_loop().run_in_executor(
                        self.executor,
                        self._next,
                        iterator_to_queue,
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


class ConcurrentFlattenAsyncIterator(_RaisingAsyncIterator[T]):
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
