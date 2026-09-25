import asyncio
from asyncio.futures import Future
from contextlib import suppress
import datetime
import sys
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from concurrent.futures import Executor, ThreadPoolExecutor
from typing import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
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

from streamable._tools._iter import AsyncClosable, ClosableAsyncIterator
from streamable._tools._observation import Observation
from streamable._tools._sentinel import STOP_ITERATION
from streamable._tools._validation import validate_async_flatten_iterable

from streamable._tools._afuture import (
    FutureResult,
    FDFOFutureResults,
    FIFOFutureResults,
    FutureResults,
)
from streamable._tools._async import AsyncFunction, anext, empty_aiter
from streamable._tools._context import NoopContextManager, aclosing
from streamable._tools._error import ExceptionContainer


T = TypeVar("T")
U = TypeVar("U")
C = TypeVar("C")
Exc = TypeVar("Exc", bound=Exception)


class _BaseOperationAsyncIterator(ClosableAsyncIterator[U], Generic[T, U]):
    """
    Provides:
    - `.aclose` closes the upstream
    - closes when exhausted
    - `.__anext__` raises `StopAsyncIteration` if closed
    """

    __slots__ = ("upstream", "_closed")

    def __init__(self, upstream: ClosableAsyncIterator[T]) -> None:
        self.upstream = upstream
        self._closed = False

    @abstractmethod
    def _anext(self) -> Awaitable[U]: ...

    async def __anext__(self) -> U:
        if self._closed:
            raise StopAsyncIteration
        try:
            return await self._anext()
        except StopAsyncIteration:
            await self.aclose()
            raise

    async def aclose(self) -> None:
        if not self._closed:
            self._closed = True
            await self.upstream.aclose()


class _RaisingAsyncIterator(
    _BaseOperationAsyncIterator[Union[T, ExceptionContainer], T]
):
    __slots__ = ()

    async def _anext(self) -> T:
        elem = await self.upstream.__anext__()
        if isinstance(elem, ExceptionContainer):
            try:
                raise elem.exception
            finally:
                del elem
        return elem


##########
# buffer #
##########


class _BufferAsyncIterable(AsyncIterable[Union[T, ExceptionContainer]]):
    __slots__ = ("upstream", "up_to")

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        up_to: Optional[int],
    ) -> None:
        self.upstream = upstream
        self.up_to = up_to or sys.maxsize

    async def _buffer_upstream(
        self,
        buffer: "asyncio.Queue[Union[T, ExceptionContainer]]",
        slots: asyncio.Semaphore,
    ) -> None:
        elem: Union[T, ExceptionContainer]
        await slots.acquire()
        stopped = False
        while not stopped:
            try:
                elem = await self.upstream.__anext__()
            except StopAsyncIteration:
                elem = STOP_ITERATION
                stopped = True
            except Exception as e:
                elem = ExceptionContainer(e)
            buffer.put_nowait(elem)
            await slots.acquire()

    async def __aiter__(self) -> AsyncGenerator[Union[T, ExceptionContainer], None]:
        async with aclosing(self.upstream):
            buffer: "asyncio.Queue[Union[T, ExceptionContainer]]" = asyncio.Queue()
            slots: asyncio.Semaphore = asyncio.Semaphore(self.up_to)
            to_yield: Deque[Union[T, ExceptionContainer]] = deque(maxlen=1)
            task = asyncio.create_task(self._buffer_upstream(buffer, slots))
            try:
                while True:
                    to_yield.append(await buffer.get())
                    if to_yield[-1] is STOP_ITERATION:
                        break
                    slots.release()
                    yield to_yield.pop()
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


class BufferAsyncIterator(_RaisingAsyncIterator[T]):
    __slots__ = ()

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        up_to: Optional[int],
    ) -> None:
        super().__init__(_BufferAsyncIterable(upstream, up_to).__aiter__())


#########
# catch #
#########


class CatchAsyncIterator(_BaseOperationAsyncIterator[T, Union[T, U]]):
    __slots__ = ("errors", "where", "replace", "do", "stop", "_stopped")

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        errors: Union[Type[Exc], Tuple[Type[Exc], ...]],
        where: Optional[AsyncFunction[Exc, object]],
        replace: Optional[AsyncFunction[Exc, U]],
        do: Optional[AsyncFunction[Exc, object]],
        stop: bool,
    ) -> None:
        super().__init__(upstream)
        self.errors = errors
        self.where = where
        self.replace = replace
        self.do = do
        self.stop = stop
        self._stopped = False

    async def _anext(self) -> Union[T, U]:
        while True:
            if self._stopped:
                raise StopAsyncIteration
            try:
                return await self.upstream.__anext__()
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


class FlattenAsyncIterator(
    _BaseOperationAsyncIterator[Union[Iterable[T], AsyncIterable[T]], T]
):
    __slots__ = ("_current_iterator_elem",)

    def __init__(
        self, upstream: ClosableAsyncIterator[Union[Iterable[T], AsyncIterable[T]]]
    ) -> None:
        super().__init__(upstream)
        self._current_iterator_elem: Union[Iterator[T], AsyncIterator[T]] = (
            empty_aiter()
        )

    async def _anext(self) -> T:
        while True:
            try:
                if isinstance(self._current_iterator_elem, AsyncIterator):
                    return await self._current_iterator_elem.__anext__()
                else:
                    return self._current_iterator_elem.__next__()
            except (StopIteration, StopAsyncIteration):
                iterable = await self.upstream.__anext__()
                validate_async_flatten_iterable(iterable)
                if isinstance(iterable, AsyncIterable):
                    self._current_iterator_elem = iterable.__aiter__()
                else:
                    self._current_iterator_elem = iterable.__iter__()


#########
# group #
#########


class GroupAsyncIterator(_BaseOperationAsyncIterator[T, List[T]]):
    __slots__ = ("up_to", "_group", "_to_raise")

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        up_to: Optional[int],
    ) -> None:
        super().__init__(upstream)
        self.up_to = up_to or cast(int, float("inf"))
        self._group: List[T] = []
        self._to_raise: Optional[Exception] = None

    async def _anext(self) -> List[T]:
        if self._to_raise:
            try:
                raise self._to_raise
            finally:
                self._to_raise = None
        while len(self._group) < self.up_to:
            try:
                self._group.append(await self.upstream.__anext__())
            except Exception as e:
                if self._group:
                    self._to_raise = e
                    break
                raise
        try:
            return self._group
        finally:
            self._group = []


class GroupByAsyncIterator(_BaseOperationAsyncIterator[T, Iterable[Tuple[U, List[T]]]]):
    __slots__ = ("up_to", "by", "_groups", "_to_raise")

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        up_to: Optional[int],
        by: AsyncFunction[T, U],
    ) -> None:
        super().__init__(upstream)
        self.up_to = up_to or cast(int, float("inf"))
        self.by = by
        self._groups: Dict[U, List[T]] = defaultdict(list)
        self._to_raise: Optional[Exception] = None

    async def _anext(self) -> Iterable[Tuple[U, List[T]]]:
        if self._to_raise:
            try:
                raise self._to_raise
            finally:
                self._to_raise = None
        while True:
            try:
                elem = await self.upstream.__anext__()
                key = await self.by(elem)
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


class _GroupByWithinAsyncIterable(
    AsyncIterable[Union[ExceptionContainer, Tuple[U, List[T]]]]
):
    __slots__ = ("upstream", "up_to", "by", "within_seconds")

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        up_to: Optional[int],
        by: AsyncFunction[T, U],
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
    async def _get_next_elem(
        next_elem: "asyncio.Queue[Union[T, ExceptionContainer]]",
        timeout: Optional[float],
    ) -> T:
        elem = await asyncio.wait_for(next_elem.get(), timeout=timeout)
        if elem is STOP_ITERATION:
            raise StopAsyncIteration
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

    async def _puller(
        self,
        next_elem: "asyncio.Queue[Union[T, ExceptionContainer]]",
        let_pull_next: asyncio.Semaphore,
    ) -> None:
        elem: Union[T, ExceptionContainer]
        await let_pull_next.acquire()
        stopped = False
        while not stopped:
            try:
                elem = await self.upstream.__anext__()
            except StopAsyncIteration:
                elem = STOP_ITERATION
                stopped = True
            except Exception as e:
                elem = ExceptionContainer(e)
            try:
                next_elem.put_nowait(elem)
            finally:
                del elem
            await let_pull_next.acquire()

    async def __aiter__(
        self,
    ) -> AsyncGenerator[Union[ExceptionContainer, Tuple[U, List[T]]], None]:
        async with aclosing(self.upstream):
            groups: Dict[U, Tuple[float, List[T]]] = defaultdict(
                lambda: (time.perf_counter(), [])
            )
            next_elem: "asyncio.Queue[Union[T, ExceptionContainer]]" = asyncio.Queue()
            let_pull_next: asyncio.Semaphore = asyncio.Semaphore(0)
            task = asyncio.create_task(self._puller(next_elem, let_pull_next))
            try:
                while True:
                    let_pull_next.release()
                    try:
                        while True:
                            try:
                                elem = await self._get_next_elem(
                                    next_elem, timeout=self._timeout(groups)
                                )
                                break
                            except asyncio.TimeoutError:
                                yield self._oldest_group(groups)
                        key = await self.by(elem)
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
                        if isinstance(e, StopAsyncIteration):
                            return
                        error = [ExceptionContainer(e)]
                    # yield outside the except block so the frame's exception state is cleared
                    yield error.pop()
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


class GroupByWithinAsyncIterator(_RaisingAsyncIterator[Tuple[U, List[T]]]):
    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        up_to: Optional[int],
        by: AsyncFunction[T, U],
        within: datetime.timedelta,
    ) -> None:
        super().__init__(
            _GroupByWithinAsyncIterable(upstream, up_to, by, within).__aiter__()
        )


########
# skip #
########


class CountSkipAsyncIterator(_BaseOperationAsyncIterator[T, T]):
    __slots__ = ("_remaining_to_skip",)

    def __init__(self, upstream: ClosableAsyncIterator[T], count: int) -> None:
        super().__init__(upstream)
        self._remaining_to_skip = count

    async def _anext(self) -> T:
        while self._remaining_to_skip > 0:
            await self.upstream.__anext__()
            # do not count exceptions as skipped elements
            self._remaining_to_skip -= 1
        return await self.upstream.__anext__()


class PredicateSkipAsyncIterator(_BaseOperationAsyncIterator[T, T]):
    __slots__ = ("until", "_satisfied")

    def __init__(
        self, upstream: ClosableAsyncIterator[T], until: AsyncFunction[T, object]
    ) -> None:
        super().__init__(upstream)
        self.until = until
        self._satisfied = False

    async def _anext(self) -> T:
        elem = await self.upstream.__anext__()
        if not self._satisfied:
            while not await self.until(elem):
                elem = await self.upstream.__anext__()
            self._satisfied = True
        return elem


########
# take #
########


class CountTakeAsyncIterator(_BaseOperationAsyncIterator[T, T]):
    __slots__ = ("_remaining_to_take",)

    def __init__(self, upstream: ClosableAsyncIterator[T], count: int) -> None:
        super().__init__(upstream)
        self._remaining_to_take = count

    async def _anext(self) -> T:
        if self._remaining_to_take <= 0:
            raise StopAsyncIteration
        elem = await self.upstream.__anext__()
        self._remaining_to_take -= 1
        return elem


class PredicateTakeAsyncIterator(_BaseOperationAsyncIterator[T, T]):
    __slots__ = ("until", "_satisfied")

    def __init__(
        self, upstream: ClosableAsyncIterator[T], until: AsyncFunction[T, object]
    ) -> None:
        super().__init__(upstream)
        self.until = until
        self._satisfied = False

    async def _anext(self) -> T:
        if self._satisfied:
            raise StopAsyncIteration
        elem = await self.upstream.__anext__()
        if await self.until(elem):
            self._satisfied = True
            return await self._anext()
        return elem


#######
# map #
#######


class MapAsyncIterator(_BaseOperationAsyncIterator[T, U]):
    __slots__ = ("into",)

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        into: AsyncFunction[T, U],
    ) -> None:
        super().__init__(upstream)
        self.into = into

    async def _anext(self) -> U:
        return await self.into(await self.upstream.__anext__())


##########
# filter #
##########


class FilterAsyncIterator(_BaseOperationAsyncIterator[T, T]):
    __slots__ = ("where",)

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        where: AsyncFunction[T, object],
    ) -> None:
        super().__init__(upstream)
        self.where = where

    async def _anext(self) -> T:
        while True:
            elem = await self.upstream.__anext__()
            if await self.where(elem):
                return elem


###########
# observe #
###########


class _BaseObserveAsyncIterator(_BaseOperationAsyncIterator[T, T]):
    __slots__ = (
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
        upstream: ClosableAsyncIterator[T],
        subject: str,
        do: AsyncFunction[Observation, object],
    ) -> None:
        super().__init__(upstream)
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

    async def _activate(self) -> None:
        self._start_point = self._time_point()
        self._active = True

    async def _observe(self) -> None:
        self._emissions_observed = self._emissions
        with suppress(Exception):
            await self.do(self._observation())

    @abstractmethod
    def _threshold(self, observed: int) -> int: ...

    async def _anext(self) -> T:
        if not self._active:
            await self._activate()
        try:
            elem = await self.upstream.__anext__()
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
        upstream: ClosableAsyncIterator[T],
        subject: str,
        do: AsyncFunction[Observation, object],
        base: int = 2,
    ) -> None:
        super().__init__(upstream, subject, do)
        self.base = base

    def _threshold(self, observed: int) -> int:
        return self.base * observed


class EveryIntObserveAsyncIterator(_BaseObserveAsyncIterator[T]):
    __slots__ = ("every",)

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        subject: str,
        every: int,
        do: AsyncFunction[Observation, object],
    ) -> None:
        super().__init__(upstream, subject, do)
        self.every = every

    def _threshold(self, observed: int) -> int:
        if not observed:
            return 0
        if observed == 1:
            return self.every
        return observed + self.every


class EveryIntervalObserveAsyncIterator(_BaseObserveAsyncIterator[T]):
    __slots__ = ("every", "_task")

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        subject: str,
        every: datetime.timedelta,
        do: AsyncFunction[Observation, object],
    ) -> None:
        super().__init__(upstream, subject, do)
        self.every = every
        self._task: Optional[asyncio.Task] = None

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
        self._task = asyncio.create_task(
            self._observer(weakref.ref(self), self.every.total_seconds())
        )
        await asyncio.sleep(0)

    def _threshold(self, observed: int) -> int:
        return cast(int, float("inf"))

    async def aclose(self) -> None:
        async with aclosing(cast(AsyncClosable, super())):
            self._active = False
            if self._task:
                self._task.cancel()
                await asyncio.gather(self._task, return_exceptions=True)


############
# throttle #
############


class ThrottleAsyncIterator(_BaseOperationAsyncIterator[T, T]):
    __slots__ = ("up_to", "_window_seconds", "_emission_timestamps")

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        up_to: int,
        per: datetime.timedelta,
    ) -> None:
        super().__init__(upstream)
        self.up_to = up_to
        self._window_seconds = per.total_seconds()
        self._emission_timestamps: Deque[float] = deque()

    async def _anext(self) -> T:
        elem: Optional[T] = None
        error: Optional[Exception] = None
        try:
            elem = await self.upstream.__anext__()
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


##################
# concurrent map #
##################


class _BaseConcurrentMapAsyncIterable(
    Generic[T, U, C],
    ABC,
    AsyncIterable[Union[U, ExceptionContainer]],
):
    __slots__ = ("upstream", "concurrency", "as_completed")

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        concurrency: int,
        as_completed: bool,
    ) -> None:
        self.upstream = upstream
        self.concurrency = concurrency
        self.as_completed = as_completed

    @abstractmethod
    def _launch_task(
        self, elem: T, context: C
    ) -> "Future[Union[U, ExceptionContainer]]": ...

    @abstractmethod
    def _task_context(self) -> ContextManager[C]: ...

    async def _next_future(
        self,
        context: C,
    ) -> Optional["Future[Union[U, ExceptionContainer]]"]:
        try:
            elem = await self.upstream.__anext__()
        except StopAsyncIteration:
            return None
        except Exception as e:
            return FutureResult(ExceptionContainer(e))
        return self._launch_task(elem, context)

    async def __aiter__(
        self,
    ) -> AsyncGenerator[Union[U, ExceptionContainer], None]:
        async with aclosing(self.upstream):
            with self._task_context() as task_context:
                future_results: FutureResults[Union[U, ExceptionContainer]] = (
                    FDFOFutureResults() if self.as_completed else FIFOFutureResults()
                )
                try:
                    # queue tasks up to buffersize
                    while len(future_results) < self.concurrency:
                        future = await self._next_future(task_context)
                        if not future:
                            # no more tasks to queue
                            break
                        future_results.add(future)
                        del future

                    # queue, wait, yield
                    while future_results:
                        future = await self._next_future(task_context)
                        if future:
                            future_results.add(future)
                            del future
                        yield await future_results.__anext__()

                finally:
                    for future in future_results.futures:
                        future.cancel()
                    await asyncio.gather(
                        *future_results.futures, return_exceptions=True
                    )
                    future_results.futures.clear()


class _AsyncConcurrentMapAsyncIterable(
    _BaseConcurrentMapAsyncIterable[T, U, asyncio.Semaphore]
):
    __slots__ = ("into",)

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        into: AsyncFunction[T, U],
        concurrency: int,
        as_completed: bool,
    ) -> None:
        super().__init__(upstream, concurrency, as_completed)
        self.into = ExceptionContainer.awrap(into)

    async def _semaphored(
        self, elem: T, semaphore: asyncio.Semaphore
    ) -> Union[U, ExceptionContainer]:
        async with semaphore:
            return await self.into(elem)

    def _launch_task(
        self, elem: T, context: asyncio.Semaphore
    ) -> "Future[Union[U, ExceptionContainer]]":
        return asyncio.create_task(self._semaphored(elem, semaphore=context))

    def _task_context(self) -> ContextManager[asyncio.Semaphore]:
        return NoopContextManager(asyncio.Semaphore(self.concurrency))


class AsyncConcurrentMapAsyncIterator(_RaisingAsyncIterator[U]):
    __slots__ = ()

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        into: AsyncFunction[T, U],
        concurrency: int,
        as_completed: bool,
    ) -> None:
        super().__init__(
            _AsyncConcurrentMapAsyncIterable(
                upstream,
                into,
                concurrency,
                as_completed,
            ).__aiter__()
        )


class _ExecutorConcurrentMapAsyncIterable(
    _BaseConcurrentMapAsyncIterable[T, U, Executor]
):
    __slots__ = ("into", "_executor")

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        into: Callable[[T], U],
        concurrency: Union[int, Executor],
        as_completed: bool,
    ) -> None:
        self.into = ExceptionContainer.wrap(into)
        self._executor: Optional[Executor] = None
        if isinstance(concurrency, int):
            super().__init__(upstream, concurrency, as_completed)
        else:
            self._executor = concurrency
            super().__init__(
                upstream, getattr(self._executor, "_max_workers"), as_completed
            )

    def _launch_task(
        self, elem: T, context: Executor
    ) -> "Future[Union[U, ExceptionContainer]]":
        return asyncio.get_running_loop().run_in_executor(context, self.into, elem)

    def _task_context(self) -> ContextManager[Executor]:
        if self._executor:
            # avoid closing client's executor
            return NoopContextManager(self._executor)
        return ThreadPoolExecutor(max_workers=self.concurrency)


class ExecutorConcurrentMapAsyncIterator(_RaisingAsyncIterator[U]):
    __slots__ = ()

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        into: Callable[[T], U],
        concurrency: Union[int, Executor],
        as_completed: bool,
    ) -> None:
        super().__init__(
            _ExecutorConcurrentMapAsyncIterable(
                upstream,
                into,
                concurrency,
                as_completed,
            ).__aiter__()
        )


######################
# concurrent flatten #
######################


class _ConcurrentFlattenAsyncIterable(AsyncIterable[Union[T, ExceptionContainer]]):
    __slots__ = ("upstream", "concurrency")

    def __init__(
        self,
        upstream: ClosableAsyncIterator[Union[Iterable[T], AsyncIterable[T]]],
        concurrency: int,
    ) -> None:
        self.upstream = upstream
        self.concurrency = concurrency

    async def __aiter__(
        self,
    ) -> AsyncGenerator[Union[T, ExceptionContainer], None]:
        async with aclosing(self.upstream):
            safe_next = ExceptionContainer.wrap(next)
            safe_anext = ExceptionContainer.awrap(anext)
            executor: Optional[Executor] = None
            iterator_and_future_pairs: Deque[
                Tuple[
                    Union[None, Iterator[T], AsyncIterator[T]],
                    Future[Union[T, ExceptionContainer]],
                ]
            ] = deque()
            to_yield: Deque[Union[T, ExceptionContainer]] = deque(maxlen=1)
            iterator_to_queue: Union[None, Iterator[T], AsyncIterator[T]] = None
            try:
                # wait, queue, yield (FIFO)
                while True:
                    if iterator_and_future_pairs:
                        iterator, future = iterator_and_future_pairs[0]
                        elem = await future
                        iterator_and_future_pairs.popleft()
                        if not isinstance(elem, ExceptionContainer) or not isinstance(
                            elem.exception, (StopIteration, StopAsyncIteration)
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
                                    iterable = await self.upstream.__anext__()
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
                            future = asyncio.create_task(safe_anext(iterator_to_queue))
                        else:
                            if not executor:
                                executor = ThreadPoolExecutor(self.concurrency)
                            future = asyncio.get_running_loop().run_in_executor(
                                executor,
                                safe_next,
                                iterator_to_queue,
                            )
                        iterator_and_future_pairs.append((iterator_to_queue, future))
                        iterator_to_queue = None
                    if to_yield:
                        yield to_yield.pop()
                    if not iterator_and_future_pairs:
                        break
            finally:
                futures = [fut for _, fut in iterator_and_future_pairs]
                for future in futures:
                    future.cancel()
                await asyncio.gather(*futures, return_exceptions=True)
                if executor:
                    executor.shutdown()


class ConcurrentFlattenAsyncIterator(_RaisingAsyncIterator[T]):
    __slots__ = ()

    def __init__(
        self,
        upstream: ClosableAsyncIterator[Union[Iterable[T], AsyncIterable[T]]],
        concurrency: int,
    ) -> None:
        super().__init__(
            _ConcurrentFlattenAsyncIterable(
                upstream,
                concurrency,
            ).__aiter__()
        )
