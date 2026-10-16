import asyncio
import sys
import threading
import time
from datetime import timedelta
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Generic,
    Iterable,
    Iterator,
    List,
    TypeVar,
    Union,
)

import pytest

from streamable import ClosableAsyncIterator, Observation, star, stream
from tests.tools.error import TestBaseError, TestError
from tests.tools.func import (
    async_identity,
    async_throw_func,
    audit_awaitable,
    identity,
    nothing,
    throw_func,
)
from tests.tools.loop import assert_only_current_task_is_alive

T = TypeVar("T")

SLOW = 0.1

##############
# MONITORING #
##############


class MonitoredIdentity(Generic[T]):
    """
    Provides an async function recording the fate of its calls (started/cancelled/completed).
    The calls get slow after the first `n_fast` calls.
    """

    def __init__(self, n_fast: int = 1) -> None:
        self.n_fast = n_fast
        self.started: List[T] = []
        self.completed: List[T] = []
        self.cancelled: List[T] = []

    async def async_func(self, arg: T) -> T:
        self.started.append(arg)
        try:
            if len(self.started) > self.n_fast:
                await asyncio.sleep(SLOW)
        except asyncio.CancelledError:
            self.cancelled.append(arg)
            raise
        self.completed.append(arg)
        return arg

    def sync_func(self, arg: T) -> T:
        self.started.append(arg)
        if len(self.started) > self.n_fast:
            time.sleep(SLOW)
        self.completed.append(arg)
        return arg


class MonitoredIntAsyncIterator(MonitoredIdentity[int], AsyncIterator[int]):
    n_closes = 0
    # pulls neither completed nor cancelled when `aclose` was called
    n_pulls_in_flight_at_close = 0

    def __init__(self, n_fast: int = 1, n_yields: int = sys.maxsize) -> None:
        super().__init__(n_fast=n_fast)
        self.n_yields = n_yields

    @property
    def closed(self) -> bool:
        return self.n_closes > 0

    async def __anext__(self) -> int:
        if len(self.started) >= self.n_yields:
            raise StopAsyncIteration
        return await self.async_func(len(self.started))

    async def aclose(self) -> None:
        self.n_closes += 1
        self.n_pulls_in_flight_at_close = (
            len(self.started) - len(self.completed) - len(self.cancelled)
        )


class MonitoredIntAsyncIterable(AsyncIterable[int]):
    def __init__(self, n_fast: int = 1, n_yields: int = sys.maxsize) -> None:
        self.n_fast = n_fast
        self.n_yields = n_yields
        self.iterators: List[MonitoredIntAsyncIterator] = []

    def __aiter__(self) -> MonitoredIntAsyncIterator:
        iterator = MonitoredIntAsyncIterator(self.n_fast, self.n_yields)
        self.iterators.append(iterator)
        return iterator


class CloseFailingMonitoredIntAsyncIterator(MonitoredIntAsyncIterator):
    async def aclose(self) -> None:
        await super().aclose()
        raise TestError()


class CloseFailingMonitoredIntAsyncIterable(AsyncIterable[int]):
    def __init__(self, n_yields: int = sys.maxsize) -> None:
        self.n_yields = n_yields
        self.iterators: List[CloseFailingMonitoredIntAsyncIterator] = []

    def __aiter__(self) -> CloseFailingMonitoredIntAsyncIterator:
        iterator = CloseFailingMonitoredIntAsyncIterator(n_yields=self.n_yields)
        self.iterators.append(iterator)
        return iterator


def chain(src: AsyncIterable[int]) -> "stream[int]":
    """Every operator flavor, downstream of a `.buffer` pulling `src` ahead."""
    return (
        stream(src)
        .buffer()
        .catch(TestError)
        .do(nothing)
        .filter(lambda _: True)
        .group(1)
        .flatten()
        .group(1, by=identity)
        .map(star(lambda _, group: group))
        .flatten(concurrency=2)
        .group(1, within=timedelta(hours=1))
        .map(stream)
        .flatten(concurrency=2)
        .group(1, by=identity, within=timedelta(hours=1))
        .map(star(lambda _, group: group))
        .flatten()
        .map(async_identity, concurrency=2)
        .map(async_identity, concurrency=2, as_completed=True)
        .map(identity, concurrency=2)
        .map(identity, concurrency=2, as_completed=True)
        .observe(do=nothing)
        .observe(every=2, do=nothing)
        .observe(every=timedelta(hours=1), do=nothing)
        .skip(1)
        .skip(until=lambda _: True)
        .take(1_000)
        .take(until=lambda _: False)
        .throttle(1_000, per=timedelta(seconds=1))
    )


################
# TERMINATIONS #
################

Termination = Callable[[ClosableAsyncIterator[Any]], Awaitable[None]]


async def close(it: ClosableAsyncIterator[Any]) -> None:
    await it.aclose()


async def expect_stop_on_next(it: ClosableAsyncIterator[Any]) -> None:
    with pytest.raises(StopAsyncIteration):
        await it.__anext__()


async def next_until_timeout(it: ClosableAsyncIterator[Any]) -> None:
    with pytest.raises(asyncio.TimeoutError):
        while True:
            await asyncio.wait_for(it.__anext__(), timeout=SLOW / 10)


async def close_while_handling_cancellation(it: ClosableAsyncIterator[Any]) -> None:
    try:
        raise asyncio.CancelledError()
    except asyncio.CancelledError:
        await it.aclose()


async def close_while_handling_base_exception(it: ClosableAsyncIterator[Any]) -> None:
    try:
        raise TestBaseError()
    except TestBaseError:
        await it.aclose()


async def expect_test_base_error_on_next(it: ClosableAsyncIterator[Any]) -> None:
    with pytest.raises(TestBaseError):
        await it.__anext__()


NON_CANCELLING_TERMINATIONS: List[Termination] = [close, expect_stop_on_next]
CANCELLING_TERMINATIONS: List[Termination] = [
    next_until_timeout,
    close_while_handling_cancellation,
    close_while_handling_base_exception,
]

#############
# ASSERTION #
#############


async def assert_pull_then_terminate_with_no_leak(
    s: "stream[Any]", n_pulls: int, termination: Termination
) -> None:
    async def pull_then_terminate() -> ClosableAsyncIterator[Any]:
        it = (s.take(n_pulls) if termination is expect_stop_on_next else s).__aiter__()
        for _ in range(n_pulls):
            await it.__anext__()
        # lets the work scheduled by the last pull start
        await asyncio.sleep(0)
        await termination(it)
        return it

    audit = await audit_awaitable(pull_then_terminate())
    assert audit.leftover_tasks == 0
    with pytest.raises(StopAsyncIteration):
        await audit.result.__anext__()
    await audit.result.aclose()  # noop
    with pytest.raises(StopAsyncIteration):
        await audit.result.__anext__()


#########
# TESTS #
#########


@pytest.mark.parametrize(
    "s",
    [
        stream(range(4)),
        stream(range(4)) + range(4),
        stream(range(4)).buffer(2),
        stream(range(4)).catch(ValueError),
        stream(range(4)).do(nothing),
        stream(range(4)).do(asyncio.sleep, concurrency=2),
        stream(range(4)).filter(),
        stream([range(2), range(2)]).flatten(),
        stream([range(2), range(2)]).flatten(concurrency=2),
        stream(range(4)).group(2),
        stream(range(4)).group(by=nothing),
        stream(range(4)).group(within=timedelta(seconds=1)),
        stream(range(4)).group(by=nothing, within=timedelta(seconds=1)),
        stream(range(4)).map(identity),
        stream(range(4)).map(asyncio.sleep, concurrency=2),
        stream(range(4)).map(identity, concurrency=2),
        stream(range(4)).observe(do=nothing),
        stream(range(4)).observe(every=2, do=nothing),
        stream(range(4)).observe(every=timedelta(seconds=1), do=nothing),
        stream(range(4)).skip(1),
        stream(range(4)).skip(until=identity),
        stream(range(4)).take(2),
        stream(range(4)).take(until=nothing),
        stream(range(4)).throttle(2, per=timedelta(seconds=1)),
    ],
)
@pytest.mark.parametrize("n_yields", [0, 1])
@pytest.mark.asyncio
async def test_close_stops_iteration(s: stream, n_yields: int) -> None:
    it = s.__aiter__()
    for _ in range(n_yields):
        await it.__anext__()
    await it.aclose()
    with pytest.raises(StopAsyncIteration):
        await it.__anext__()


@pytest.mark.parametrize(
    "termination", NON_CANCELLING_TERMINATIONS + CANCELLING_TERMINATIONS
)
@pytest.mark.asyncio
async def test_map_termination(termination: Termination) -> None:
    src = MonitoredIntAsyncIterator()
    await assert_pull_then_terminate_with_no_leak(
        stream(src).map(identity), n_pulls=1, termination=termination
    )
    # nothing is in flight but the pull of a cancelled `anext`, the source is not closed
    assert (src.completed, src.cancelled, src.closed) == ([0], src.started[1:], False)


@pytest.mark.parametrize(
    "termination", NON_CANCELLING_TERMINATIONS + CANCELLING_TERMINATIONS
)
@pytest.mark.asyncio
async def test_source_termination(termination: Termination) -> None:
    # the source iterator is closed if the stream created it
    iterable = MonitoredIntAsyncIterable()
    await assert_pull_then_terminate_with_no_leak(
        stream(iterable), n_pulls=1, termination=termination
    )
    assert [iterator.closed for iterator in iterable.iterators] == [True]

    # but not if it was provided by the client
    iterator = MonitoredIntAsyncIterator()
    await assert_pull_then_terminate_with_no_leak(
        stream(iterator), n_pulls=1, termination=termination
    )
    assert not iterator.closed


@pytest.mark.asyncio
async def test_owned_source_close_error_is_raised() -> None:
    it = stream(CloseFailingMonitoredIntAsyncIterable()).__aiter__()
    assert await it.__anext__() == 0

    # the TestError is raised on close
    with pytest.raises(TestError):
        await it.aclose()

    # the TestError is raised on stop
    with pytest.raises(TestError):
        await stream(CloseFailingMonitoredIntAsyncIterable()).take(1)


@pytest.mark.parametrize(
    "termination", NON_CANCELLING_TERMINATIONS + CANCELLING_TERMINATIONS
)
@pytest.mark.asyncio
async def test_owned_source_closed_once(termination: Termination) -> None:
    # each operator propagates the closure upstream, but the source is closed once
    iterable = MonitoredIntAsyncIterable()
    s = stream(iterable).map(identity).filter(lambda _: True).do(nothing)
    await assert_pull_then_terminate_with_no_leak(s, n_pulls=1, termination=termination)
    assert [iterator.n_closes for iterator in iterable.iterators] == [1]


@pytest.mark.asyncio
async def test_owned_source_closed_once_on_upstream_stop() -> None:
    # a stop right above the source is propagated by every operator above it, but the source is closed once
    iterable = MonitoredIntAsyncIterable()
    s = stream(iterable).take(1).map(identity).filter(lambda _: True).do(nothing)
    it = s.__aiter__()
    assert [elem async for elem in it] == [0]
    await it.aclose()
    assert [iterator.n_closes for iterator in iterable.iterators] == [1]

    # a failing close is not retried: its error is raised once, then the iteration stops
    failing_iterable = CloseFailingMonitoredIntAsyncIterable()
    s = stream(failing_iterable).take(1).map(identity).filter(lambda _: True)
    it = s.__aiter__()
    assert await it.__anext__() == 0
    with pytest.raises(TestError):
        await it.__anext__()
    with pytest.raises(StopAsyncIteration):
        await it.__anext__()
    await it.aclose()
    assert [iterator.n_closes for iterator in failing_iterable.iterators] == [1]


@pytest.mark.parametrize(
    "operate",
    [
        lambda s: s.map(throw_func(TestError)),
        lambda s: s.map(async_throw_func(TestError), concurrency=2),
        lambda s: s.map(throw_func(TestError), concurrency=2),
        lambda s: s.buffer(2).map(throw_func(TestError)),
    ],
)
@pytest.mark.asyncio
async def test_await_closes_owned_source_on_error(
    operate: Callable[["stream[int]"], "stream[int]"],
) -> None:
    iterable = MonitoredIntAsyncIterable()
    # await stream
    audit = await audit_awaitable(operate(stream(iterable)), assert_raises=TestError)
    assert audit.leftover_tasks == 0
    assert [iterator.n_closes for iterator in iterable.iterators] == [1]


@pytest.mark.parametrize(
    "operate",
    [
        lambda s: s.map(throw_func(TestError)),
        lambda s: s.map(async_throw_func(TestError), concurrency=2),
        lambda s: s.map(throw_func(TestError), concurrency=2),
        lambda s: s.buffer(2).map(throw_func(TestError)),
    ],
)
def test_call_closes_owned_source_on_error(
    operate: Callable[["stream[int]"], "stream[int]"],
) -> None:
    iterable = MonitoredIntAsyncIterable()
    # call stream
    with pytest.raises(TestError):
        operate(stream(iterable))()
    assert [iterator.n_closes for iterator in iterable.iterators] == [1]


@pytest.mark.parametrize(
    "termination", NON_CANCELLING_TERMINATIONS + CANCELLING_TERMINATIONS
)
@pytest.mark.parametrize("as_completed", [False, True])
@pytest.mark.asyncio
async def test_map_termination_with_concurrency(
    termination: Termination, as_completed: bool, caplog: pytest.LogCaptureFixture
) -> None:
    monitored_identity = MonitoredIdentity[int]()
    s = stream(range(9)).map(
        monitored_identity.async_func, concurrency=2, as_completed=as_completed
    )
    await assert_pull_then_terminate_with_no_leak(s, n_pulls=1, termination=termination)
    # the in-flight calls are cancelled, whatever ends the iteration
    assert (
        monitored_identity.completed == [0]
        and monitored_identity.cancelled == monitored_identity.started[1:] != []
    )
    # no error surfaced from the futures' done callbacks
    assert not caplog.records


@pytest.mark.parametrize("termination", NON_CANCELLING_TERMINATIONS)
@pytest.mark.asyncio
async def test_buffer_close(termination: Termination) -> None:
    src = MonitoredIntAsyncIterator()
    await assert_pull_then_terminate_with_no_leak(
        stream(src).buffer(1), n_pulls=1, termination=termination
    )
    # the in-flight pull is awaited, not cancelled, and the source is not closed
    assert (src.completed, src.cancelled, src.closed) == ([0, 1], [], False)

    # idle because full, it does not pull again
    src = MonitoredIntAsyncIterator(n_fast=2)
    await assert_pull_then_terminate_with_no_leak(
        stream(src).buffer(1), n_pulls=1, termination=termination
    )
    assert src.started == [0, 1]


@pytest.mark.parametrize("termination", CANCELLING_TERMINATIONS)
@pytest.mark.asyncio
async def test_buffer_cancel(termination: Termination) -> None:
    src = MonitoredIntAsyncIterator()
    await assert_pull_then_terminate_with_no_leak(
        stream(src).buffer(1), n_pulls=1, termination=termination
    )
    # the in-flight pull is cancelled, the source is not closed
    assert (src.completed, src.cancelled, src.closed) == ([0], [1], False)


@pytest.mark.asyncio
async def test_buffer_task_cancel() -> None:
    it = stream(MonitoredIntAsyncIterator()).buffer(1).__aiter__()
    await it.__anext__()
    await asyncio.sleep(0)  # the puller's next pull is in flight
    background_tasks = asyncio.all_tasks() - {asyncio.current_task()}
    assert len(background_tasks) == 1
    puller_task = background_tasks.pop()
    puller_task.cancel()
    # should not hang
    await asyncio.wait_for(
        asyncio.gather(puller_task, return_exceptions=True), timeout=1
    )


@pytest.mark.parametrize("termination", NON_CANCELLING_TERMINATIONS)
@pytest.mark.asyncio
async def test_group_close(termination: Termination) -> None:
    src = MonitoredIntAsyncIterator()
    # the group is yielded on `within` timeout, while the pull of its next element is in flight
    await assert_pull_then_terminate_with_no_leak(
        stream(src).group(within=timedelta(seconds=SLOW / 5)),
        n_pulls=1,
        termination=termination,
    )
    # the in-flight pull is awaited, not cancelled, and the source is not closed
    assert (src.completed, src.cancelled, src.closed) == ([0, 1], [], False)

    # idle because the group is yielded on size, it does not pull again
    src = MonitoredIntAsyncIterator()
    await assert_pull_then_terminate_with_no_leak(
        stream(src).group(1, within=timedelta(hours=1)),
        n_pulls=1,
        termination=termination,
    )
    assert src.started == [0]


@pytest.mark.parametrize("termination", CANCELLING_TERMINATIONS)
@pytest.mark.asyncio
async def test_group_cancel(termination: Termination) -> None:
    src = MonitoredIntAsyncIterator()
    await assert_pull_then_terminate_with_no_leak(
        stream(src).group(within=timedelta(seconds=SLOW / 5)),
        n_pulls=1,
        termination=termination,
    )
    # the in-flight pull is cancelled, the source is not closed
    assert (src.completed, src.cancelled, src.closed) == ([0], [1], False)


@pytest.mark.parametrize("termination", NON_CANCELLING_TERMINATIONS)
@pytest.mark.asyncio
async def test_flatten_provided_inner_iter_close(termination: Termination) -> None:
    a, b, c = (
        MonitoredIntAsyncIterator(),
        MonitoredIntAsyncIterator(),
        MonitoredIntAsyncIterator(),
    )
    s = stream[AsyncIterator[int]]([a, b, c]).flatten(concurrency=2)
    await assert_pull_then_terminate_with_no_leak(s, n_pulls=2, termination=termination)
    # the in-flight pulls are awaited, not cancelled, and the inner iterators are not closed
    for src in (a, b):
        assert (src.completed, src.cancelled, src.closed) == ([0, 1], [], False)
    assert c.started == []


@pytest.mark.parametrize("termination", CANCELLING_TERMINATIONS)
@pytest.mark.asyncio
async def test_flatten_provided_inner_iter_cancel(termination: Termination) -> None:
    a, b, c = (
        MonitoredIntAsyncIterator(),
        MonitoredIntAsyncIterator(),
        MonitoredIntAsyncIterator(),
    )
    s = stream[AsyncIterator[int]]([a, b, c]).flatten(concurrency=2)
    await assert_pull_then_terminate_with_no_leak(s, n_pulls=2, termination=termination)
    # the in-flight pulls are cancelled, the inner iterators are not closed
    for src in (a, b):
        assert (src.completed, src.cancelled, src.closed) == ([0], [1], False)
    assert c.started == []


@pytest.mark.parametrize("termination", NON_CANCELLING_TERMINATIONS)
@pytest.mark.parametrize("concurrency", [1, 2])
@pytest.mark.asyncio
async def test_flatten_owned_inner_iter_close(
    termination: Termination, concurrency: int
) -> None:
    src = MonitoredIntAsyncIterator()
    s = stream([stream(src).buffer(1)]).flatten(concurrency=concurrency)
    await assert_pull_then_terminate_with_no_leak(s, n_pulls=1, termination=termination)
    # outside of any cancellation context: closing the owned inner stream awaits its in-flight pull
    assert src.started[:2] == [0, 1]
    assert (src.completed, src.cancelled, src.closed) == (src.started, [], False)


@pytest.mark.parametrize("concurrency", [1, 2])
@pytest.mark.asyncio
async def test_flatten_owned_inner_iter_close_error_is_silent(concurrency: int) -> None:
    # the owned inner iters closure is done in best effort, errors are silenced
    iterable = CloseFailingMonitoredIntAsyncIterable()
    # on close:
    s = stream[AsyncIterable[int]]([iterable]).flatten(concurrency=concurrency)
    it = s.__aiter__()
    assert await it.__anext__() == 0
    await it.aclose()
    assert iterable.iterators[-1].closed is True

    # on stop:
    it = s.take(1).__aiter__()
    assert await it.__anext__() == 0
    with pytest.raises(StopAsyncIteration):
        await it.__anext__()
    assert iterable.iterators[-1].closed is True

    # on exhaustion:
    iterables = [CloseFailingMonitoredIntAsyncIterable(n_yields=1) for _ in range(2)]
    s = stream[AsyncIterable[int]](iterables).flatten(concurrency=concurrency)
    assert [elem async for elem in s] == [0, 0]
    assert [
        iterator.closed for iterable in iterables for iterator in iterable.iterators
    ] == [True, True]


@pytest.mark.parametrize("termination", CANCELLING_TERMINATIONS)
@pytest.mark.parametrize("concurrency", [1, 2])
@pytest.mark.asyncio
async def test_flatten_owned_inner_iter_cancel(
    termination: Termination, concurrency: int
) -> None:
    src = MonitoredIntAsyncIterator()
    s = stream([stream(src).buffer(1)]).flatten(concurrency=concurrency)
    await assert_pull_then_terminate_with_no_leak(s, n_pulls=1, termination=termination)
    # within cancellation context: closing the owned inner stream iter cancels its in-flight pull
    assert (src.completed, src.cancelled, src.closed) == ([0], [1], False)


@pytest.mark.asyncio
async def test_flatten_interrupt() -> None:
    async def interrupted_after_first_yielding() -> AsyncIterator[int]:
        yield 0
        raise TestBaseError()

    src = MonitoredIntAsyncIterator(n_fast=1)
    s = stream[AsyncIterator[int]]([interrupted_after_first_yielding(), src]).flatten(
        concurrency=2
    )
    await assert_pull_then_terminate_with_no_leak(
        s, n_pulls=2, termination=expect_test_base_error_on_next
    )
    # a `BaseException` raised by an inner iterator cancels the in-flight pulls of the others
    assert (src.completed, src.cancelled, src.closed) == ([0], [1], False)


@pytest.mark.parametrize(
    "termination", NON_CANCELLING_TERMINATIONS + CANCELLING_TERMINATIONS
)
@pytest.mark.parametrize(
    "concurrency, src_getters",
    [
        (1, [MonitoredIntAsyncIterable]),
        (1, [MonitoredIntAsyncIterator]),
        (
            2,
            [
                MonitoredIntAsyncIterable,
                MonitoredIntAsyncIterator,
                MonitoredIntAsyncIterable,
            ],
        ),
        (
            3,
            [
                MonitoredIntAsyncIterable,
                MonitoredIntAsyncIterator,
                MonitoredIntAsyncIterable,
            ],
        ),
        (3, [lambda: [0], MonitoredIntAsyncIterable, MonitoredIntAsyncIterator]),
    ],
)
@pytest.mark.asyncio
async def test_flatten_both_owned_and_provided_inner_iters(
    termination: Termination,
    concurrency: int,
    src_getters: List[Callable[[], Union[AsyncIterable[int], Iterable[int]]]],
) -> None:
    src = [get_src() for get_src in src_getters]
    s = stream[Union[AsyncIterable[int], Iterable[int]]](src).flatten(
        concurrency=concurrency
    )
    await assert_pull_then_terminate_with_no_leak(s, n_pulls=2, termination=termination)
    # in-flight owned iterators are closed
    assert all(
        iterator.closed
        for iterable in src[:concurrency]
        if isinstance(iterable, MonitoredIntAsyncIterable)
        for iterator in iterable.iterators
    )
    # in-flight client-provided iterators are not
    assert not any(
        iterable.closed
        for iterable in src[:concurrency]
        if isinstance(iterable, MonitoredIntAsyncIterator)
    )


@pytest.mark.parametrize("termination", NON_CANCELLING_TERMINATIONS)
@pytest.mark.asyncio
async def test_add_close(termination: Termination) -> None:
    await assert_pull_then_terminate_with_no_leak(
        stream(MonitoredIntAsyncIterator()).buffer(1) + [9],
        n_pulls=1,
        termination=termination,
    )


@pytest.mark.parametrize(
    "termination", NON_CANCELLING_TERMINATIONS + CANCELLING_TERMINATIONS
)
@pytest.mark.asyncio
async def test_observe_termination_has_no_leak(termination: Termination) -> None:
    monitored_identity = MonitoredIdentity[Observation](n_fast=0)
    s = stream(MonitoredIntAsyncIterator()).observe(
        every=timedelta(seconds=SLOW),
        do=monitored_identity.async_func,
    )
    await assert_pull_then_terminate_with_no_leak(s, n_pulls=1, termination=termination)
    # the in-flight observation is cancelled, whatever ends the iteration
    assert monitored_identity.cancelled == monitored_identity.started != []


@pytest.mark.parametrize("termination", NON_CANCELLING_TERMINATIONS)
@pytest.mark.parametrize("n", [0, 1])
@pytest.mark.asyncio
async def test_chain_close(termination: Termination, n: int) -> None:
    src = MonitoredIntAsyncIterator(n_fast=20)
    await assert_pull_then_terminate_with_no_leak(
        chain(src), n_pulls=n, termination=termination
    )
    # every operator got closed, the in-flight pull is awaited, the source is not closed
    assert (src.completed, src.cancelled, src.closed) == (src.started, [], False)


@pytest.mark.parametrize("termination", CANCELLING_TERMINATIONS)
@pytest.mark.asyncio
async def test_chain_cancellation(
    termination: Termination,
) -> None:
    src = MonitoredIntAsyncIterator(n_fast=20)
    await assert_pull_then_terminate_with_no_leak(
        chain(src), n_pulls=1, termination=termination
    )
    # the cancellation reaches the in-flight pull through every operator
    assert (src.completed + src.cancelled, src.closed) == (src.started, False)
    assert len(src.cancelled) == 1


@pytest.mark.parametrize(
    "termination", NON_CANCELLING_TERMINATIONS + CANCELLING_TERMINATIONS
)
@pytest.mark.parametrize("n_pulls", [0, 1])
@pytest.mark.asyncio
async def test_chain_with_owned_source_termination(
    termination: Termination, n_pulls: int
) -> None:
    iterable = MonitoredIntAsyncIterable(n_fast=20)
    await assert_pull_then_terminate_with_no_leak(
        chain(iterable), n_pulls=n_pulls, termination=termination
    )
    # the closure reaches the source through every operator, even the ones not started yet,
    # and closes it once its in-flight pull is done
    assert [
        (iterator.n_closes, iterator.n_pulls_in_flight_at_close)
        for iterator in iterable.iterators
    ] == [(1, 0)]


@pytest.mark.asyncio
async def test_cancel_during_cancel() -> None:
    class SlowToCancel(MonitoredIntAsyncIterator):
        async def __anext__(self) -> int:
            try:
                return await super().__anext__()
            except asyncio.CancelledError:
                await asyncio.sleep(1)
                raise

    src = SlowToCancel()
    closing = asyncio.ensure_future(
        assert_pull_then_terminate_with_no_leak(
            stream(src).buffer(1),
            n_pulls=1,
            termination=close_while_handling_cancellation,
        )
    )
    await asyncio.sleep(SLOW / 10)
    # the close waits for the source to honor the cancellation
    assert src.cancelled == [1] and not closing.done()
    # a second cancellation is not swallowed
    closing.cancel()
    audit_closing = await audit_awaitable(closing, assert_raises=asyncio.CancelledError)
    assert audit_closing.duration < SLOW / 10
    assert_only_current_task_is_alive()


###########
# THREADS #
###########


async def max_event_loop_stall(coro: Awaitable[None]) -> float:
    """Awaits `coro` and returns the longest time the event loop went without running."""
    task = asyncio.ensure_future(coro)
    stall = 0.0
    while not task.done():
        start = time.perf_counter()
        await asyncio.sleep(0)
        stall = max(stall, time.perf_counter() - start)
    await task
    return stall


THREAD_BACKED_STREAM_GETTERS: List[Callable[[MonitoredIdentity], "stream[int]"]] = [
    lambda monitored_identity: stream(range(9)).map(
        monitored_identity.sync_func, concurrency=2
    ),
    lambda monitored_identity: stream(range(9)).map(
        monitored_identity.sync_func, concurrency=2, as_completed=True
    ),
    lambda monitored_identity: stream[Iterator[int]](
        [map(monitored_identity.sync_func, range(9)) for _ in range(2)]
    ).flatten(concurrency=2),
]


@pytest.mark.parametrize("get_stream", THREAD_BACKED_STREAM_GETTERS)
@pytest.mark.parametrize("termination", NON_CANCELLING_TERMINATIONS)
@pytest.mark.asyncio
async def test_close_executor_backed(
    get_stream: Callable[[MonitoredIdentity[int]], "stream[int]"],
    termination: Termination,
) -> None:
    monitored_identity = MonitoredIdentity[int](n_fast=0)
    threads = set(threading.enumerate())
    stall = await max_event_loop_stall(
        assert_pull_then_terminate_with_no_leak(
            get_stream(monitored_identity), n_pulls=1, termination=termination
        )
    )
    # the in-flight calls are awaited
    assert sorted(monitored_identity.completed) == sorted(monitored_identity.started)
    # without blocking the event loop
    assert stall < SLOW / 10
    # threads are joined
    assert set(threading.enumerate()) == threads


@pytest.mark.parametrize("get_stream", THREAD_BACKED_STREAM_GETTERS)
@pytest.mark.parametrize("termination", CANCELLING_TERMINATIONS)
@pytest.mark.asyncio
async def test_cancel_executor_backed(
    get_stream: Callable[[MonitoredIdentity[int]], "stream[int]"],
    termination: Termination,
) -> None:
    monitored_identity = MonitoredIdentity[int](n_fast=0)
    threads = set(threading.enumerate())
    await assert_pull_then_terminate_with_no_leak(
        get_stream(monitored_identity), n_pulls=1, termination=termination
    )
    # the in-flight calls are abandoned: their threads are left behind
    assert set(threading.enumerate()) > threads
    # ... and terminate on their own
    await asyncio.sleep(3 * SLOW)
    assert sorted(monitored_identity.completed) == sorted(monitored_identity.started)
    assert set(threading.enumerate()) == threads
