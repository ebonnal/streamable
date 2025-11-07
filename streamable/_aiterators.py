import asyncio
import datetime
import multiprocessing
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from concurrent.futures import Executor, Future, ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import suppress
from math import ceil
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    ContextManager,
    Coroutine,
    DefaultDict,
    Deque,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from streamable._util._asynctools import awaitable_to_coroutine, empty_aiter
from streamable._util._contextmanagertools import noop_context_manager
from streamable._util._errortools import ExceptionContainer
from streamable._util._loggertools import get_logger

from streamable._util._constants import NO_REPLACEMENT
from streamable._util._futuretools import (
    AsyncFDFOFutureResultCollection,
    AsyncFIFOFutureResultCollection,
    FutureResult,
    FutureResultCollection,
)

with suppress(ImportError):
    from typing import Literal

T = TypeVar("T")
U = TypeVar("U")


#########
# catch #
#########


class ACatchAsyncIterator(AsyncIterator[T]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        when: Optional[Callable[[Exception], Coroutine[Any, Any, Any]]],
        replacement: T,
        finally_raise: bool,
    ) -> None:
        self.iterator = iterator
        self.errors = errors
        self.when = when
        self.replacement = replacement
        self.finally_raise = finally_raise
        self._to_be_finally_raised: Optional[Exception] = None

    async def __anext__(self) -> T:
        while True:
            try:
                return await self.iterator.__anext__()
            except StopAsyncIteration:
                if self._to_be_finally_raised:
                    try:
                        raise self._to_be_finally_raised
                    finally:
                        self._to_be_finally_raised = None
                raise
            except self.errors as e:
                if not self.when or await self.when(e):
                    if self.finally_raise and not self._to_be_finally_raised:
                        self._to_be_finally_raised = e
                    if self.replacement is not NO_REPLACEMENT:
                        return self.replacement
                    continue
                raise


############
# distinct #
############


class ADistinctAsyncIterator(AsyncIterator[T]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        key: Optional[Callable[[T], Coroutine[Any, Any, Any]]],
    ) -> None:
        self.iterator = iterator
        self.key = key
        self._already_seen: Set[Any] = set()

    async def __anext__(self) -> T:
        while True:
            elem = await self.iterator.__anext__()
            key = await self.key(elem) if self.key else elem
            if key not in self._already_seen:
                break
        self._already_seen.add(key)
        return elem


class ConsecutiveADistinctAsyncIterator(AsyncIterator[T]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        key: Optional[Callable[[T], Coroutine[Any, Any, Any]]],
    ) -> None:
        self.iterator = iterator
        self.key = key
        self._last_key: Any = object()

    async def __anext__(self) -> T:
        while True:
            elem = await self.iterator.__anext__()
            key = await self.key(elem) if self.key else elem
            if key != self._last_key:
                break
        self._last_key = key
        return elem


###########
# flatten #
###########


class FlattenAsyncIterator(AsyncIterator[U]):
    def __init__(self, iterator: AsyncIterator[Iterable[U]]) -> None:
        self.iterator = iterator
        self._current_iterator_elem: Iterator[U] = tuple().__iter__()

    async def __anext__(self) -> U:
        while True:
            try:
                return self._current_iterator_elem.__next__()
            except StopIteration:
                self._current_iterator_elem = (
                    await self.iterator.__anext__()
                ).__iter__()


class AFlattenAsyncIterator(AsyncIterator[U]):
    def __init__(self, iterator: AsyncIterator[AsyncIterable[U]]) -> None:
        self.iterator = iterator
        self._current_iterator_elem: AsyncIterator[U] = empty_aiter()

    async def __anext__(self) -> U:
        while True:
            try:
                return await self._current_iterator_elem.__anext__()
            except StopAsyncIteration:
                self._current_iterator_elem = (
                    await self.iterator.__anext__()
                ).__aiter__()


#########
# group #
#########


class _BaseGroupAsyncIterator(Generic[T]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        size: Optional[int],
        interval: Optional[datetime.timedelta],
    ) -> None:
        self.iterator = iterator
        self.size = size or cast(int, float("inf"))
        self.interval = interval
        self._interval_seconds = interval.total_seconds() if interval else float("inf")
        self._to_be_raised: Optional[Exception] = None
        self._last_group_yielded_at: float = 0

    def _interval_seconds_have_elapsed(self) -> bool:
        if not self.interval:
            return False
        return (
            time.perf_counter() - self._last_group_yielded_at
        ) >= self._interval_seconds

    def _remember_group_time(self) -> None:
        if self.interval:
            self._last_group_yielded_at = time.perf_counter()

    def _init_last_group_time(self) -> None:
        if self.interval and not self._last_group_yielded_at:
            self._last_group_yielded_at = time.perf_counter()


class GroupAsyncIterator(_BaseGroupAsyncIterator[T], AsyncIterator[List[T]]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        size: Optional[int],
        interval: Optional[datetime.timedelta],
    ) -> None:
        super().__init__(iterator, size, interval)
        self._current_group: List[T] = []

    async def __anext__(self) -> List[T]:
        self._init_last_group_time()
        if self._to_be_raised:
            try:
                raise self._to_be_raised
            finally:
                self._to_be_raised = None
        try:
            while len(self._current_group) < self.size and (
                not self._interval_seconds_have_elapsed() or not self._current_group
            ):
                self._current_group.append(await self.iterator.__anext__())
        except Exception as e:
            if not self._current_group:
                raise
            self._to_be_raised = e

        group, self._current_group = self._current_group, []
        self._remember_group_time()
        return group


class AGroupbyAsyncIterator(
    _BaseGroupAsyncIterator[T], AsyncIterator[Tuple[U, List[T]]]
):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        key: Callable[[T], Coroutine[Any, Any, U]],
        size: Optional[int],
        interval: Optional[datetime.timedelta],
    ) -> None:
        super().__init__(iterator, size, interval)
        self.key = key
        self._is_exhausted = False
        self._groups_by: DefaultDict[U, List[T]] = defaultdict(list)

    async def _group_next_elem(self) -> None:
        elem = await self.iterator.__anext__()
        self._groups_by[await self.key(elem)].append(elem)

    def _pop_full_group(self) -> Optional[Tuple[U, List[T]]]:
        for key, group in self._groups_by.items():
            if len(group) >= self.size:
                return key, self._groups_by.pop(key)
        return None

    def _pop_first_group(self) -> Tuple[U, List[T]]:
        first_key: U = self._groups_by.__iter__().__next__()
        return first_key, self._groups_by.pop(first_key)

    def _pop_largest_group(self) -> Tuple[U, List[T]]:
        largest_group_key: Any = self._groups_by.__iter__().__next__()

        for key, group in self._groups_by.items():
            if len(group) > len(self._groups_by[largest_group_key]):
                largest_group_key = key

        return largest_group_key, self._groups_by.pop(largest_group_key)

    async def __anext__(self) -> Tuple[U, List[T]]:
        self._init_last_group_time()
        if self._is_exhausted:
            if self._groups_by:
                return self._pop_first_group()
            raise StopAsyncIteration

        if self._to_be_raised:
            if self._groups_by:
                self._remember_group_time()
                return self._pop_first_group()
            try:
                raise self._to_be_raised
            finally:
                self._to_be_raised = None

        try:
            await self._group_next_elem()

            full_group: Optional[Tuple[U, List[T]]] = self._pop_full_group()
            while not full_group and not self._interval_seconds_have_elapsed():
                await self._group_next_elem()
                full_group = self._pop_full_group()

            self._remember_group_time()
            return full_group or self._pop_largest_group()

        except StopAsyncIteration:
            self._is_exhausted = True
            return await self.__anext__()

        except Exception as e:
            self._to_be_raised = e
            return await self.__anext__()


########
# skip #
########


class CountSkipAsyncIterator(AsyncIterator[T]):
    def __init__(self, iterator: AsyncIterator[T], count: int) -> None:
        self.iterator = iterator
        self.count = count
        self._n_skipped = 0
        self._done_skipping = False

    async def __anext__(self) -> T:
        if not self._done_skipping:
            while self._n_skipped < self.count:
                await self.iterator.__anext__()
                # do not count exceptions as skipped elements
                self._n_skipped += 1
            self._done_skipping = True
        return await self.iterator.__anext__()


class PredicateASkipAsyncIterator(AsyncIterator[T]):
    def __init__(
        self, iterator: AsyncIterator[T], until: Callable[[T], Coroutine[Any, Any, Any]]
    ) -> None:
        self.iterator = iterator
        self.until = until
        self._done_skipping = False

    async def __anext__(self) -> T:
        elem = await self.iterator.__anext__()
        if not self._done_skipping:
            while not await self.until(elem):
                elem = await self.iterator.__anext__()
            self._done_skipping = True
        return elem


class CountAndPredicateASkipAsyncIterator(AsyncIterator[T]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        count: int,
        until: Callable[[T], Coroutine[Any, Any, Any]],
    ) -> None:
        self.iterator = iterator
        self.count = count
        self.until = until
        self._n_skipped = 0
        self._done_skipping = False

    async def __anext__(self) -> T:
        elem = await self.iterator.__anext__()
        if not self._done_skipping:
            while self._n_skipped < self.count and not await self.until(elem):
                elem = await self.iterator.__anext__()
                # do not count exceptions as skipped elements
                self._n_skipped += 1
            self._done_skipping = True
        return elem


############
# truncate #
############


class CountTruncateAsyncIterator(AsyncIterator[T]):
    def __init__(self, iterator: AsyncIterator[T], count: int) -> None:
        self.iterator = iterator
        self.count = count
        self._current_count = 0

    async def __anext__(self) -> T:
        if self._current_count == self.count:
            raise StopAsyncIteration()
        elem = await self.iterator.__anext__()
        self._current_count += 1
        return elem


class PredicateATruncateAsyncIterator(AsyncIterator[T]):
    def __init__(
        self, iterator: AsyncIterator[T], when: Callable[[T], Coroutine[Any, Any, Any]]
    ) -> None:
        self.iterator = iterator
        self.when = when
        self._satisfied = False

    async def __anext__(self) -> T:
        if self._satisfied:
            raise StopAsyncIteration()
        elem = await self.iterator.__anext__()
        if await self.when(elem):
            self._satisfied = True
            raise StopAsyncIteration()
        return elem


#######
# map #
#######


class AMapAsyncIterator(AsyncIterator[U]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        transformation: Callable[[T], Coroutine[Any, Any, U]],
    ) -> None:
        self.iterator = iterator
        self.transformation = transformation

    async def __anext__(self) -> U:
        return await self.transformation(await self.iterator.__anext__())


##########
# filter #
##########


class AFilterAsyncIterator(AsyncIterator[T]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        when: Callable[[T], Coroutine[Any, Any, Any]],
    ) -> None:
        self.iterator = iterator
        self.when = when

    async def __anext__(self) -> T:
        while True:
            elem = await self.iterator.__anext__()
            if await self.when(elem):
                return elem


###########
# observe #
###########


class ObserveAsyncIterator(AsyncIterator[T]):
    def __init__(self, iterator: AsyncIterator[T], what: str, base: int = 2) -> None:
        self.iterator = iterator
        self.what = what
        self.base = base
        self._yields = 0
        self._errors = 0
        self._nexts_logged = 0
        self._yields_logged = 0
        self._errors_logged = 0
        self._started_time: Optional[datetime.datetime] = None
        self._logger = get_logger()
        self._format = f"[duration=%s errors=%s] %s {what} yielded"

    def _log(self) -> None:
        now = datetime.datetime.fromtimestamp(time.perf_counter())
        duration = now - cast(datetime.datetime, self._started_time)
        self._logger.info(self._format, duration, self._errors, self._yields)

    async def __anext__(self) -> T:
        if not self._started_time:
            self._started_time = datetime.datetime.fromtimestamp(time.perf_counter())
        try:
            elem = await self.iterator.__anext__()
            self._yields += 1
            if self._yields >= self.base * self._yields_logged:
                self._log()
                self._yields_logged = self._yields
                self._nexts_logged = self._yields + self._errors
            return elem
        except StopAsyncIteration:
            if self._yields + self._errors > self._nexts_logged:
                self._log()
            raise
        except Exception:
            self._errors += 1
            if self._errors >= self.base * self._errors_logged:
                self._log()
                self._errors_logged = self._errors
                self._nexts_logged = self._yields + self._errors
            raise


############
# throttle #
############


class YieldsPerPeriodThrottleAsyncIterator(AsyncIterator[T]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        max_yields: int,
        period: datetime.timedelta,
    ) -> None:
        self.iterator = iterator
        self.max_yields = max_yields
        self._period_seconds = period.total_seconds()
        self._period_index: int = -1
        self._yields_in_period = 0
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
            self._yields_in_period = max(0, self._yields_in_period - self.max_yields)

        if self._yields_in_period >= self.max_yields:
            await asyncio.sleep(
                (ceil(num_periods) - num_periods) * self._period_seconds
            )
        self._yields_in_period += 1

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
        buffersize: int,
        ordered: bool,
    ) -> None:
        self.iterator = iterator
        self.buffersize = buffersize
        self.ordered = ordered

    def _context_manager(self) -> ContextManager:
        return noop_context_manager()

    @abstractmethod
    def _launch_task(self, elem: T) -> "Future[Union[U, ExceptionContainer]]": ...

    def _future_result_collection(
        self,
    ) -> FutureResultCollection[Union[U, ExceptionContainer]]:
        if self.ordered:
            return AsyncFIFOFutureResultCollection(asyncio.get_running_loop())
        else:
            return AsyncFDFOFutureResultCollection(asyncio.get_running_loop())

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
        with self._context_manager():
            future_results = self._future_result_collection()

            # queue tasks up to buffersize
            while len(future_results) < self.buffersize:
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
        transformation: Callable[[T], U],
        concurrency: int,
        buffersize: int,
        ordered: bool,
        via: "Literal['thread', 'process']",
    ) -> None:
        super().__init__(iterator, buffersize, ordered)
        self.transformation = transformation
        self.concurrency = concurrency
        self.executor: Executor
        self.via = via

    def _context_manager(self) -> ContextManager:
        if self.via == "thread":
            self.executor = ThreadPoolExecutor(max_workers=self.concurrency)
        elif self.via == "process":
            self.executor = ProcessPoolExecutor(
                max_workers=self.concurrency,
                mp_context=multiprocessing.get_context("spawn"),
            )
        return self.executor

    # picklable
    @staticmethod
    def _safe_transformation(
        transformation: Callable[[T], U], elem: T
    ) -> Union[U, ExceptionContainer]:
        try:
            return transformation(elem)
        except Exception as e:
            return ExceptionContainer(e)

    def _launch_task(self, elem: T) -> "Future[Union[U, ExceptionContainer]]":
        return cast(
            "Future[Union[U, ExceptionContainer]]",
            asyncio.get_running_loop().run_in_executor(
                self.executor, self._safe_transformation, self.transformation, elem
            ),
        )


class ConcurrentMapAsyncIterator(_RaisingAsyncIterator[U]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        transformation: Callable[[T], U],
        concurrency: int,
        buffersize: int,
        ordered: bool,
        via: "Literal['thread', 'process']",
    ) -> None:
        super().__init__(
            _ConcurrentMapAsyncIterable(
                iterator,
                transformation,
                concurrency,
                buffersize,
                ordered,
                via,
            ).__aiter__()
        )


class _ConcurrentAMapAsyncIterable(_BaseConcurrentMapAsyncIterable[T, U]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        transformation: Callable[[T], Coroutine[Any, Any, U]],
        concurrency: int,
        buffersize: int,
        ordered: bool,
    ) -> None:
        super().__init__(iterator, buffersize, ordered)
        self.transformation = transformation
        self.concurrency = concurrency
        self._semaphore: Optional[asyncio.Semaphore] = None

    @property
    def semaphore(self) -> asyncio.Semaphore:
        if not self._semaphore:
            self._semaphore = asyncio.Semaphore(self.concurrency)
        return self._semaphore

    async def _safe_transformation(self, elem: T) -> Union[U, ExceptionContainer]:
        try:
            async with self.semaphore:
                return await self.transformation(elem)
        except Exception as e:
            return ExceptionContainer(e)

    def _launch_task(self, elem: T) -> "Future[Union[U, ExceptionContainer]]":
        return cast(
            "Future[Union[U, ExceptionContainer]]",
            asyncio.get_running_loop().create_task(self._safe_transformation(elem)),
        )


class ConcurrentAMapAsyncIterator(_RaisingAsyncIterator[U]):
    def __init__(
        self,
        iterator: AsyncIterator[T],
        transformation: Callable[[T], Coroutine[Any, Any, U]],
        concurrency: int,
        buffersize: int,
        ordered: bool,
    ) -> None:
        super().__init__(
            _ConcurrentAMapAsyncIterable(
                iterator,
                transformation,
                concurrency,
                buffersize,
                ordered,
            ).__aiter__()
        )


######################
# concurrent flatten #
######################


class _ConcurrentFlattenAsyncIterable(AsyncIterable[Union[T, ExceptionContainer]]):
    def __init__(
        self,
        iterables_iterator: AsyncIterator[Iterable[T]],
        concurrency: int,
        buffersize: int,
    ) -> None:
        self.iterables_iterator = iterables_iterator
        self.concurrency = concurrency
        self.buffersize = buffersize
        self._next = ExceptionContainer.wrap(next)

    async def __aiter__(
        self,
    ) -> AsyncIterator[Union[T, ExceptionContainer]]:
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            iterator_and_future_pairs: Deque[
                Tuple[
                    Optional[Iterator[T]],
                    "Awaitable[Union[T, ExceptionContainer]]",
                ]
            ] = deque()
            to_yield: Deque[Union[T, ExceptionContainer]] = deque(maxlen=1)
            iterator_to_queue: Optional[Iterator[T]] = None
            # wait, queue, yield (FIFO)
            while True:
                if iterator_and_future_pairs:
                    iterator, future = iterator_and_future_pairs.popleft()
                    elem = await future
                    if not isinstance(elem, ExceptionContainer) or not isinstance(
                        elem.exception, StopIteration
                    ):
                        to_yield.append(elem)
                        iterator_to_queue = iterator

                # queue tasks up to buffersize
                while len(iterator_and_future_pairs) < self.buffersize:
                    if not iterator_to_queue:
                        try:
                            try:
                                iterable = await self.iterables_iterator.__anext__()
                            except StopAsyncIteration:
                                break
                            iterator_to_queue = iterable.__iter__()
                        except Exception as e:
                            iterator_to_queue = None
                            future = FutureResult(ExceptionContainer(e))
                            iterator_and_future_pairs.append(
                                (iterator_to_queue, future)
                            )
                            continue
                    future = asyncio.get_running_loop().run_in_executor(
                        executor, self._next, iterator_to_queue
                    )
                    iterator_and_future_pairs.append((iterator_to_queue, future))
                    iterator_to_queue = None
                if to_yield:
                    yield to_yield.pop()
                if not iterator_and_future_pairs:
                    break


class ConcurrentFlattenAsyncIterator(_RaisingAsyncIterator[T]):
    def __init__(
        self,
        iterables_iterator: AsyncIterator[Iterable[T]],
        concurrency: int,
        buffersize: int,
    ) -> None:
        super().__init__(
            _ConcurrentFlattenAsyncIterable(
                iterables_iterator,
                concurrency,
                buffersize,
            ).__aiter__()
        )


class _ConcurrentAFlattenAsyncIterable(AsyncIterable[Union[T, ExceptionContainer]]):
    def __init__(
        self,
        iterables_iterator: AsyncIterator[AsyncIterable[T]],
        concurrency: int,
        buffersize: int,
    ) -> None:
        self.iterables_iterator = iterables_iterator
        self.concurrency = concurrency
        self.buffersize = buffersize

    async def __aiter__(
        self,
    ) -> AsyncIterator[Union[T, ExceptionContainer]]:
        iterator_and_future_pairs: Deque[
            Tuple[
                Optional[AsyncIterator[T]],
                Awaitable[Union[T, ExceptionContainer]],
            ]
        ] = deque()
        to_yield: Deque[Union[T, ExceptionContainer]] = deque(maxlen=1)
        iterator_to_queue: Optional[AsyncIterator[T]] = None
        # wait, queue, yield (FIFO)
        while True:
            if iterator_and_future_pairs:
                iterator, future = iterator_and_future_pairs.popleft()
                try:
                    to_yield.append(await future)
                    iterator_to_queue = iterator
                except StopAsyncIteration:
                    pass
                except Exception as e:
                    to_yield.append(ExceptionContainer(e))
                    iterator_to_queue = iterator

            # queue tasks up to buffersize
            while len(iterator_and_future_pairs) < self.buffersize:
                if not iterator_to_queue:
                    try:
                        try:
                            iterable = await self.iterables_iterator.__anext__()
                        except StopAsyncIteration:
                            break
                        iterator_to_queue = iterable.__aiter__()
                    except Exception as e:
                        iterator_to_queue = None
                        future = FutureResult(ExceptionContainer(e))
                        iterator_and_future_pairs.append((iterator_to_queue, future))
                        continue
                future = asyncio.get_running_loop().create_task(
                    awaitable_to_coroutine(iterator_to_queue.__anext__())
                )
                iterator_and_future_pairs.append((iterator_to_queue, future))
                iterator_to_queue = None
            if to_yield:
                yield to_yield.pop()
            if not iterator_and_future_pairs:
                break


class ConcurrentAFlattenAsyncIterator(_RaisingAsyncIterator[T]):
    def __init__(
        self,
        iterables_iterator: AsyncIterator[AsyncIterable[T]],
        concurrency: int,
        buffersize: int,
    ) -> None:
        super().__init__(
            _ConcurrentAFlattenAsyncIterable(
                iterables_iterator,
                concurrency,
                buffersize,
            ).__aiter__()
        )
