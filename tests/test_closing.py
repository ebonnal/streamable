import asyncio
from datetime import timedelta
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Generic,
    List,
    TypeVar,
)

import pytest

from streamable import ClosableAsyncIterator, Observation, star, stream
from streamable._tools._async import anext
from tests.tools.error import TestBaseError, TestError
from tests.tools.func import async_identity, audit_awaitable, identity, nothing
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


class MonitoredIntAsyncIterator(MonitoredIdentity[int], AsyncIterator[int]):
    closed = False

    async def __anext__(self) -> int:
        return await self.async_func(len(self.started))

    async def aclose(self) -> None:
        self.closed = True


def chain(src: AsyncIterator[int]) -> "stream[int]":
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
        await anext(it)


async def next_until_timeout(it: ClosableAsyncIterator[Any]) -> None:
    with pytest.raises(asyncio.TimeoutError):
        while True:
            await asyncio.wait_for(anext(it), timeout=SLOW / 10)


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
        await anext(it)


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
            await anext(it)
        # lets the work scheduled by the last pull start
        await asyncio.sleep(0)
        await termination(it)
        return it

    audit = await audit_awaitable(pull_then_terminate())
    assert audit.leftover_tasks == 0
    with pytest.raises(StopAsyncIteration):
        await anext(audit.result)
    await audit.result.aclose()  # noop
    with pytest.raises(StopAsyncIteration):
        await anext(audit.result)


#########
# TESTS #
#########


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
    await anext(it)
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
async def test_flatten_close(termination: Termination) -> None:
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
async def test_flatten_cancel(termination: Termination) -> None:
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


@pytest.mark.parametrize("termination", NON_CANCELLING_TERMINATIONS)
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="`+` flattens its operands and `.flatten` does not close inner iterators",
)
@pytest.mark.asyncio
async def test_add_close(termination: Termination) -> None:
    # for now the `.buffer`'s puller task is left behind
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
