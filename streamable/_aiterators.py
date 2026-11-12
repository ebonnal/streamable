import asyncio
import datetime
import time
import weakref
from abc import abstractmethod
from collections import defaultdict, deque
from concurrent import futures
from concurrent.futures import Executor, ThreadPoolExecutor
from contextlib import suppress
from typing import (
    AsyncIterable,
    AsyncIterator,
    Callable,
    Deque,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from streamable._tools._afuture import (
    FailedFuture,
    FDFOFutureResults,
    FIFOFutureResults,
    FutureResults,
    cancel_threadsafe,
    ensure_done,
)
from streamable._tools._async import AsyncFunction, anext, semaphored
from streamable._tools._error import ExceptionContainer
from streamable._tools._iter import (
    ClosableAsyncIterator,
    GuardedAsyncIterator,
    GuardedIterator,
    SyncToClosableAsyncIterator,
    empty_aiter,
    safe_aclose_fn,
)
from streamable._tools._observation import Observation
from streamable._tools._sentinel import STOP_ITERATION
from streamable._tools._validation import validate_iterable

T = TypeVar("T")
U = TypeVar("U")
Exc = TypeVar("Exc", bound=Exception)


class _BaseOperationAsyncIterator(ClosableAsyncIterator[U], Generic[T, U]):
    """
    Provides `.aclose` of owned upstream, and unwraps upstream if it's a `GuardedAsyncIterator`.
    """

    __slots__ = (
        "upstream",
        "_closed",
        "_safe_aclose_upstream",
        "_closure_attempted",
    )

    def __init__(self, upstream: ClosableAsyncIterator[T]) -> None:
        self.upstream: AsyncIterator[T]
        if isinstance(upstream, GuardedAsyncIterator):
            self.upstream = upstream.iterator
            self._safe_aclose_upstream = upstream.safe_aclose_iterator
        else:
            self.upstream = upstream
            self._safe_aclose_upstream = safe_aclose_fn(upstream)
        self._closed = False
        self._closure_attempted = False

    async def aclose(self) -> None:
        if not self._closed:
            await self._safe_aclose_upstream()
            self._closed = True

    async def _attempt_aclose_then_stop(self) -> None:
        try:
            raise StopAsyncIteration
        finally:
            if not self._closure_attempted:
                self._closure_attempted = True
                await self.aclose()


class _Puller(Generic[T]):
    __slots__ = (
        "_upstream",
        "elems",
        "_semaphore",
        "_pulling_task",
        "_stopper_task",
        "_stopped",
    )

    def __init__(
        self,
        upstream: AsyncIterator[T],
        ahead_pulls: Optional[int],
    ) -> None:
        self._upstream = upstream
        self.elems: "asyncio.Queue[Union[T, ExceptionContainer]]" = asyncio.Queue()
        self._semaphore = (
            None if ahead_pulls is None else asyncio.Semaphore(ahead_pulls)
        )
        self._stopped = False
        self._stopper_task = asyncio.create_task(self._stopper())
        self._pulling_task = asyncio.create_task(self._pulling())

    def allow_to_pull(self) -> None:
        if self._semaphore:
            self._semaphore.release()

    async def _ask_to_pull(self) -> None:
        if self._semaphore is None:
            # unbounded buffer over a never suspending source would starve the loop
            await asyncio.sleep(0)
        else:
            await self._semaphore.acquire()

    async def _stopper(self) -> None:
        try:
            await asyncio.sleep(float("inf"))
        except asyncio.CancelledError:
            self._stopped = True
            self.allow_to_pull()
            await ensure_done(self._pulling_task, cancel=True)

    async def _pulling(self) -> None:
        await self._ask_to_pull()
        while not self._stopped:
            try:
                self.elems.put_nowait(await self._upstream.__anext__())
            except StopAsyncIteration:
                self.stop()
                self.elems.put_nowait(STOP_ITERATION)
                continue
            except BaseException as e:
                if not self._stopped:
                    self.elems.put_nowait(ExceptionContainer(e))
            await self._ask_to_pull()

    def stop(self) -> None:
        if not self._stopped:
            self._stopped = True
            self._stopper_task.cancel()

    def stop_threadsafe(self) -> None:
        with suppress(RuntimeError):
            self._stopper_task.get_loop().call_soon_threadsafe(
                self._stopper_task.cancel
            )

    async def wait(self) -> None:
        await ensure_done(
            (
                self._stopper_task,
                self._pulling_task,
            )
        )

    def clear(self) -> None:
        while not self.elems.empty():
            self.elems.get_nowait()


class _PullerBasedAsyncIterator(_BaseOperationAsyncIterator[T, U]):
    __slots__ = ("_ahead_pulls", "_stopped", "_puller")

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        ahead_pulls: Optional[int],
    ) -> None:
        super().__init__(upstream)
        self._ahead_pulls = ahead_pulls
        self._puller: Optional[_Puller] = None
        self._stopped = False

    @property
    def _lazy_puller(self) -> _Puller[T]:
        if self._puller is None:
            self._puller = _Puller(
                self.upstream,
                self._ahead_pulls,
            )
        return self._puller

    async def _terminate(self) -> None:
        self._stopped = True
        if self._puller:
            self._puller.stop()
            await self._puller.wait()
            self._puller.clear()
            self._puller.elems.put_nowait(STOP_ITERATION)

    async def aclose(self) -> None:
        if not self._closed:
            await self._terminate()
            await self._safe_aclose_upstream()
            self._closed = True

    def __del__(self) -> None:
        if self._puller:
            self._puller.stop_threadsafe()
            self._puller.clear()


##########
# buffer #
##########


class BufferAsyncIterator(_PullerBasedAsyncIterator[T, T]):
    __slots__ = ()

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        up_to: Optional[int],
    ) -> None:
        super().__init__(upstream, up_to)

    async def __anext__(self) -> T:
        if self._stopped:
            raise StopAsyncIteration
        # a cancelled `.get` takes no element: release only once one is taken
        elem = await self._lazy_puller.elems.get()
        self._lazy_puller.allow_to_pull()
        if elem is STOP_ITERATION:
            await self._terminate()
            raise StopAsyncIteration
        try:
            return ExceptionContainer.raise_if_instance(elem)
        finally:
            del elem


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

    async def __anext__(self) -> Union[T, U]:
        while True:
            if self._stopped:
                await self._attempt_aclose_then_stop()
            try:
                return await self.upstream.__anext__()
            except StopAsyncIteration:
                raise
            except self.errors as e:
                if self.where is None or await self.where(e):
                    if self.stop:
                        self._stopped = True
                    if self.do is not None:
                        await self.do(e)
                    if self.replace is not None:
                        return await self.replace(e)
                    continue
                raise


###########
# flatten #
###########


class FlattenAsyncIterator(
    _BaseOperationAsyncIterator[Union[Iterable[T], AsyncIterable[T]], T]
):
    __slots__ = ("_iterator", "_iterator_owned")

    def __init__(
        self,
        upstream: ClosableAsyncIterator[Union[Iterable[T], AsyncIterable[T]]],
    ) -> None:
        super().__init__(upstream)
        self._iterator: Optional[AsyncIterator[T]] = None
        self._iterator_owned: bool = False

    async def __anext__(self) -> T:
        while True:
            try:
                return await (self._iterator or empty_aiter).__anext__()
            except StopAsyncIteration:
                self._iterator = None
                iterable = await self.upstream.__anext__()
                validate_iterable(iterable)
                if hasattr(type(iterable), "__aiter__"):
                    self._iterator_owned = not hasattr(type(iterable), "__anext__")
                    self._iterator = cast(AsyncIterable[T], iterable).__aiter__()
                else:
                    self._iterator_owned = True
                    iterator = cast(Iterable[T], iterable).__iter__()
                    self._iterator = SyncToClosableAsyncIterator(
                        iterator, owned=not hasattr(type(iterable), "__next__")
                    )

    async def aclose(self) -> None:
        if not self._closed:
            if self._iterator_owned:
                self._iterator_owned = False
                with suppress(Exception):
                    await safe_aclose_fn(self._iterator)()
            await self._safe_aclose_upstream()
            self._closed = True


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

    async def __anext__(self) -> List[T]:
        if self._to_raise is not None:
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

    async def __anext__(self) -> Iterable[Tuple[U, List[T]]]:
        if self._to_raise is not None:
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


class GroupByWithinAsyncIterator(_PullerBasedAsyncIterator[T, Tuple[U, List[T]]]):
    __slots__ = (
        "up_to",
        "by",
        "within_seconds",
        "_groups",
        "_to_raise",
        "_pending_pull",
    )

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        up_to: Optional[int],
        by: AsyncFunction[T, U],
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
        self._pending_pull: "Optional[asyncio.Task[Union[T, ExceptionContainer]]]" = (
            None
        )

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

    async def __anext__(self) -> Tuple[U, List[T]]:
        if self._to_raise is not None:
            if self._groups:
                return self._oldest_group(self._groups)
            try:
                raise self._to_raise
            finally:
                self._to_raise = None
        if self._stopped:
            raise StopAsyncIteration
        while True:
            try:
                if self._pending_pull is None:
                    self._lazy_puller.allow_to_pull()
                    self._pending_pull = asyncio.create_task(
                        self._lazy_puller.elems.get()
                    )
                timeout = self._timeout(self._groups)
                if not await ensure_done(self._pending_pull, timeout=timeout):
                    return self._oldest_group(self._groups)
                try:
                    elem = ExceptionContainer.raise_if_instance(
                        self._pending_pull.result()
                    )
                finally:
                    self._pending_pull = None
                if elem is STOP_ITERATION:
                    await self._terminate()
                    raise StopAsyncIteration
                key = await self.by(elem)
                _, group = self._groups[key]
                group.append(elem)
                if len(group) == self.up_to:
                    del self._groups[key]
                    return (key, group)
            except Exception as e:
                if self._groups:
                    self._to_raise = e
                    return await self.__anext__()
                raise

    async def _terminate(self) -> None:
        await ensure_done(self._pending_pull, cancel=True)
        self._pending_pull = None
        await super()._terminate()

    def __del__(self) -> None:
        super().__del__()
        cancel_threadsafe(self._pending_pull)


########
# skip #
########


class CountSkipAsyncIterator(_BaseOperationAsyncIterator[T, T]):
    __slots__ = ("_remaining_to_skip",)

    def __init__(self, upstream: ClosableAsyncIterator[T], count: int) -> None:
        super().__init__(upstream)
        self._remaining_to_skip = count

    async def __anext__(self) -> T:
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

    async def __anext__(self) -> T:
        elem = await self.upstream.__anext__()
        if not self._satisfied:
            while not await self.until(elem):
                elem = await self.upstream.__anext__()
            self._satisfied = True
        return elem


############
# take #
############


class CountTakeAsyncIterator(_BaseOperationAsyncIterator[T, T]):
    __slots__ = ("_remaining_to_take",)

    def __init__(self, upstream: ClosableAsyncIterator[T], count: int) -> None:
        super().__init__(upstream)
        self._remaining_to_take = count

    async def __anext__(self) -> T:
        if self._remaining_to_take <= 0:
            await self._attempt_aclose_then_stop()
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

    async def __anext__(self) -> T:
        if self._satisfied:
            await self._attempt_aclose_then_stop()
        elem = await self.upstream.__anext__()
        if await self.until(elem):
            self._satisfied = True
            return await self.__anext__()
        return elem


#######
# map #
#######


class MapAsyncIterator(_BaseOperationAsyncIterator[T, U]):
    __slots__ = ("into",)

    def __init__(
        self, upstream: ClosableAsyncIterator[T], into: AsyncFunction[T, U]
    ) -> None:
        super().__init__(upstream)
        self.into = into

    async def __anext__(self) -> U:
        return await self.into(await self.upstream.__anext__())


##########
# filter #
##########


class FilterAsyncIterator(_BaseOperationAsyncIterator[T, T]):
    __slots__ = ("where",)

    def __init__(
        self, upstream: ClosableAsyncIterator[T], where: AsyncFunction[T, object]
    ) -> None:
        super().__init__(upstream)
        self.where = where

    async def __anext__(self) -> T:
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
        "_activated",
        "_start_point",
        "_do_base_exception",
        "_stopped",
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
        self._activated = False
        self._start_point: float
        self._do_base_exception: Optional[BaseException] = None
        self._stopped: asyncio.Event

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

    async def _activate(self) -> None:
        self._start_point = time.perf_counter()
        self._stopped = asyncio.Event()
        self._activated = True

    async def _observe(self) -> None:
        self._emissions_observed = self._emissions
        try:
            await self.do(self._observation())
        except Exception:
            pass
        except asyncio.CancelledError:
            raise
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

    async def __anext__(self) -> T:
        if not self._activated:
            await self._activate()
        try:
            elem = await self.upstream.__anext__()
            self._elements += 1
            if self._elements >= self._threshold(self._elements_observed):
                await self._observe()
                self._elements_observed = self._elements
            return elem
        except StopAsyncIteration:
            if not self._stopped.is_set() and (
                not self._emissions or self._emissions > self._emissions_observed
            ):
                await self._observe()
            await self._terminate()
            raise
        except Exception:
            self._errors += 1
            if self._errors >= self._threshold(self._errors_observed):
                await self._observe()
                self._errors_observed = self._errors
            raise
        finally:
            self._reraise_do_base_exception()

    async def _terminate(self) -> None:
        self._stopped.set()


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
    __slots__ = ("__weakref__", "every", "_task")

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        subject: str,
        every: datetime.timedelta,
        do: AsyncFunction[Observation, object],
    ) -> None:
        super().__init__(upstream, subject, do)
        self.every = every
        self._task: Optional["asyncio.Task[None]"] = None

    @staticmethod
    async def _observer(
        weak_self: "weakref.ReferenceType[EveryIntervalObserveAsyncIterator[T]]",
        every_seconds: float,
        stopped: asyncio.Event,
    ) -> None:
        self = weak_self()
        wait_stop = asyncio.create_task(stopped.wait())
        try:
            while self and not stopped.is_set():
                await self._observe()
                self = None
                await ensure_done(wait_stop, timeout=every_seconds)
                self = weak_self()
        finally:
            stopped.set()

    async def _activate(self) -> None:
        await super()._activate()
        if self._closed:
            self._stopped.set()
            return
        self._task = asyncio.create_task(
            self._observer(
                weakref.ref(self),
                self.every.total_seconds(),
                self._stopped,
            )
        )
        await asyncio.sleep(0)

    def _threshold(self, observed: int) -> int:
        return cast(int, float("inf"))

    async def aclose(self) -> None:
        if not self._closed:
            if self._activated:
                await self._terminate()
            await self._safe_aclose_upstream()
            self._closed = True

    async def _terminate(self) -> None:
        await super()._terminate()
        await ensure_done(self._task)

    def __del__(self) -> None:
        if self._task is not None:
            with suppress(RuntimeError):
                self._task.get_loop().call_soon_threadsafe(self._stopped.set)


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

    async def _sleep(self) -> None:
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

    async def __anext__(self) -> T:
        try:
            elem = await self.upstream.__anext__()
            await self._sleep()
            return elem
        except StopAsyncIteration:
            raise
        except Exception:
            await self._sleep()
            raise


##########################
# concurrent map/flatten #
##########################


class _ConcurrentAsyncIterator(_BaseOperationAsyncIterator[T, U]):
    __slots__ = (
        "concurrency",
        "_executor",
        "_stopped",
        "_executor_owned",
    )

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
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
            self._executor_owned = False
            self.concurrency = getattr(self._executor, "_max_workers")

    @property
    def _lazy_executor(self) -> Executor:
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=self.concurrency)
        return self._executor

    def _shutdown_executor(self, wait: bool = True) -> None:
        if self._executor and self._executor_owned:
            self._executor.shutdown(wait=wait)

    @abstractmethod
    async def _terminate(self) -> None: ...

    @abstractmethod
    def _cancel_threadsafe(self) -> None: ...

    async def aclose(self) -> None:
        if not self._closed:
            await self._terminate()
            await self._safe_aclose_upstream()
            self._closed = True

    def __del__(self) -> None:
        self._cancel_threadsafe()
        self._shutdown_executor(wait=False)


class _ConcurrentMapAsyncIterator(_ConcurrentAsyncIterator[T, U]):
    __slots__ = ("as_completed", "_future_results")

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        concurrency: Union[int, Executor],
        as_completed: bool,
    ) -> None:
        self.as_completed = as_completed
        self._future_results: Optional[FutureResults[U]] = None
        super().__init__(upstream, concurrency)

    @property
    def _lazy_future_results(self) -> FutureResults[U]:
        if self._future_results is None:
            self._future_results = (
                FDFOFutureResults() if self.as_completed else FIFOFutureResults()
            )
        return self._future_results

    @abstractmethod
    def _launch_future(
        self, elem: T
    ) -> "Union[asyncio.Future[U], futures.Future[U]]": ...

    async def _next_future(
        self,
    ) -> "Optional[Union[asyncio.Future[U], futures.Future[U]]]":
        try:
            elem = await self.upstream.__anext__()
        except StopAsyncIteration:
            return None
        except Exception as e:
            return FailedFuture(e)
        return self._launch_future(elem)

    async def __anext__(self) -> U:
        if self._stopped:
            raise StopAsyncIteration
        future_results = self._lazy_future_results
        while len(future_results) < self.concurrency + 1:
            future = await self._next_future()
            if future is None:
                # no more tasks to queue
                break
            future_results.add(future)
            del future
        if not future_results:
            await self._terminate()
            return await self.__anext__()
        return await future_results.__anext__()

    async def _terminate(self) -> None:
        self._stopped = True
        if self._future_results:
            self._future_results.cancel()
            await self._future_results.gather()
            self._future_results.clear()
        self._shutdown_executor()

    def _cancel_threadsafe(self) -> None:
        if self._future_results:
            self._future_results.cancel_threadsafe()
            self._future_results.clear()


class AsyncConcurrentMapAsyncIterator(_ConcurrentMapAsyncIterator[T, U]):
    __slots__ = ("into", "_semaphore")

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        into: AsyncFunction[T, U],
        concurrency: Union[int, Executor],
        as_completed: bool,
    ) -> None:
        self.into = into
        self._semaphore: Optional[asyncio.Semaphore] = None
        super().__init__(upstream, concurrency, as_completed)

    @property
    def _lazy_semaphore(self) -> asyncio.Semaphore:
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.concurrency)
        return self._semaphore

    def _launch_future(self, elem: T) -> "asyncio.Future[U]":
        return asyncio.create_task(semaphored(self.into, elem, self._lazy_semaphore))


class ExecutorConcurrentMapAsyncIterator(_ConcurrentMapAsyncIterator[T, U]):
    __slots__ = ("into",)

    def __init__(
        self,
        upstream: ClosableAsyncIterator[T],
        into: Callable[[T], U],
        concurrency: Union[int, Executor],
        as_completed: bool,
    ) -> None:
        self.into = into
        super().__init__(upstream, concurrency, as_completed)

    def _launch_future(self, elem: T) -> "futures.Future[U]":
        return self._lazy_executor.submit(self.into, elem)


class ConcurrentFlattenAsyncIterator(
    _ConcurrentAsyncIterator[Union[Iterable[T], AsyncIterable[T]], T]
):
    __slots__ = ("_iterators_and_futures",)

    def __init__(
        self,
        upstream: ClosableAsyncIterator[Union[Iterable[T], AsyncIterable[T]]],
        concurrency: Union[int, ThreadPoolExecutor],
    ) -> None:
        self._iterators_and_futures: Deque[
            Tuple[
                Union[GuardedAsyncIterator[T], GuardedIterator[T], None],
                "Union[asyncio.Future[T], futures.Future[T]]",
            ]
        ] = deque()
        super().__init__(upstream, concurrency)

    def _launch_future_from_iter(
        self, iterator: Union[GuardedAsyncIterator[T], GuardedIterator[T]]
    ) -> "Union[asyncio.Future[T], futures.Future[T]]":
        if isinstance(iterator, GuardedAsyncIterator):
            return asyncio.create_task(anext(iterator, STOP_ITERATION))
        return self._lazy_executor.submit(next, iterator, STOP_ITERATION)

    async def _launch_future_from_next_iter(
        self,
    ) -> Optional[
        Tuple[
            Union[GuardedAsyncIterator[T], GuardedIterator[T], None],
            "Union[asyncio.Future[T], futures.Future[T]]",
        ]
    ]:
        try:
            iterable = await self.upstream.__anext__()
        except StopAsyncIteration:
            return None
        except Exception as e:
            return (None, FailedFuture(e))
        validate_iterable(iterable)
        iterator: Union[GuardedAsyncIterator[T], GuardedIterator[T]] = (
            GuardedAsyncIterator(iterable, silent_close=True)
            if isinstance(iterable, AsyncIterable)
            else GuardedIterator(iterable, silent_close=True)
        )
        return (iterator, self._launch_future_from_iter(iterator))

    async def __anext__(self) -> T:
        while True:
            if self._stopped:
                raise StopAsyncIteration
            while len(self._iterators_and_futures) < self.concurrency:
                iterator_and_future = await self._launch_future_from_next_iter()
                if iterator_and_future is None:
                    # no more tasks to queue
                    break
                self._iterators_and_futures.append(iterator_and_future)
            if not self._iterators_and_futures:
                await self._terminate()
                continue
            iterator, future = self._iterators_and_futures[0]
            await ensure_done(future)
            self._iterators_and_futures.popleft()
            try:
                elem = future.result()
                if elem is STOP_ITERATION:
                    iterator = None
                    continue
                return elem
            finally:
                del future
                if iterator is not None:
                    self._iterators_and_futures.append(
                        (iterator, self._launch_future_from_iter(iterator))
                    )

    async def _terminate(self) -> None:
        self._stopped = True
        await ensure_done((fut for _, fut in self._iterators_and_futures), cancel=True)
        for iterator, _ in self._iterators_and_futures:
            if isinstance(iterator, GuardedAsyncIterator):
                await iterator.aclose()
            elif iterator is not None:
                iterator.close()
        self._iterators_and_futures.clear()
        self._shutdown_executor()

    def _cancel_threadsafe(self) -> None:
        cancel_threadsafe(future for _, future in self._iterators_and_futures)
