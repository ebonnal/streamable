import asyncio
import threading
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import (
    Any,
    Awaitable,
    Callable,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
    cast,
)

import pytest

from streamable import ClosableAsyncIterator, ClosableIterator, star, stream
from tests.tools.error import TestError
from tests.tools.func import (
    async_identity,
    async_throw_func,
    audit_awaitable,
    identity,
    nothing,
    throw_func,
)
from tests.tools.iter import (
    ITERABLE_TYPES,
    IterableType,
    aclose_or_close,
    aiter_or_iter,
    alist_or_list,
    anext_or_next,
    stopiteration_type,
)
from tests.tools.loop import TEST_LOOP, assert_only_current_task_is_alive
from tests.tools.monitor import (
    SLOW,
    CloseFailingMonitoredIntIterable,
    MonitoredIdentity,
    MonitoredIntIterable,
    MonitoredIntIterator,
)

T = TypeVar("T")


def chain(src: Iterable[int], itype: IterableType) -> "stream[int]":
    """Every operator flavor, downstream of a `.buffer` pulling `src` ahead."""
    # a sync iteration involving no coroutine function goes through the sync operators
    concurrent_identity: Callable[[Any], Any] = (
        async_identity if itype == "async" else identity
    )
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
        .map(concurrent_identity, concurrency=2)
        .map(concurrent_identity, concurrency=2, as_completed=True)
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


async def _aexpect_stop_on_next(it: ClosableAsyncIterator[Any]) -> None:
    with pytest.raises(StopAsyncIteration):
        await it.__anext__()


def _expect_stop_on_next(it: ClosableIterator[Any]) -> None:
    with pytest.raises(StopIteration):
        next(it)


async def _aclose(it: ClosableAsyncIterator[Any]) -> None:
    await it.aclose()


def _close(it: ClosableIterator[Any]) -> None:
    it.close()


async def _anext_until_timeout_then_close(it: ClosableAsyncIterator[Any]) -> None:
    # a timeout does not end the iteration, the close does
    with pytest.raises(asyncio.TimeoutError):
        while True:
            await asyncio.wait_for(it.__anext__(), timeout=SLOW / 10)
    await it.aclose()


@dataclass(frozen=True)
class Termination:
    name: str
    aterminate: Callable[[ClosableAsyncIterator[Any]], Awaitable[None]]
    # `None` if the termination has no sync equivalent
    terminate: Optional[Callable[[ClosableIterator[Any]], None]]
    # whether the stream must be stopped by a `.take` right after the pulls
    stops: bool = False


EXPECT_STOP_ON_NEXT = Termination(
    "expect_stop_on_next", _aexpect_stop_on_next, _expect_stop_on_next, stops=True
)
CLOSE = Termination("close", _aclose, _close)
NEXT_UNTIL_TIMEOUT_THEN_CLOSE = Termination(
    "next_until_timeout_then_close", _anext_until_timeout_then_close, None
)

TERMINATIONS: List[Termination] = [
    EXPECT_STOP_ON_NEXT,
    CLOSE,
    NEXT_UNTIL_TIMEOUT_THEN_CLOSE,
]

ITYPES_X_TERMINATIONS = [
    pytest.param(itype, termination, id=f"{itype}-{termination.name}")
    for itype in ITERABLE_TYPES
    for termination in TERMINATIONS
    if itype == "async" or termination.terminate is not None
]


def with_itypes(
    cases: List[Any], async_only_cases: List[Any] = []
) -> List[Tuple[IterableType, Any]]:
    """`(itype, case)` pairs, with the `async_only_cases` paired with `"async"` only."""
    pairs: List[Tuple[IterableType, Any]] = [
        (itype, case) for itype in ITERABLE_TYPES for case in cases
    ]
    pairs.extend(("async", case) for case in async_only_cases)
    return pairs


#############
# ASSERTION #
#############


def assert_no_new_threads(threads: Set[threading.Thread]) -> None:
    # a thread signaling it is done may take a moment to leave `threading.enumerate()`
    deadline = time.perf_counter() + SLOW / 10
    while set(threading.enumerate()) != threads and time.perf_counter() < deadline:
        time.sleep(SLOW / 1000)
    assert set(threading.enumerate()) == threads


async def apull_then_terminate_with_no_leak(
    s: "stream[Any]", n_pulls: int, termination: Termination
) -> ClosableAsyncIterator[Any]:
    async def pull_then_terminate() -> ClosableAsyncIterator[Any]:
        it = (s.take(n_pulls) if termination.stops else s).__aiter__()
        for _ in range(n_pulls):
            await it.__anext__()
        # lets the work scheduled by the last pull start
        await asyncio.sleep(0)
        await termination.aterminate(it)
        return it

    audit = await audit_awaitable(pull_then_terminate())
    assert audit.leftover_tasks == 0
    await audit.result.aclose()  # noop
    return audit.result


def pull_then_terminate_with_no_leak(
    s: "stream[Any]", n_pulls: int, termination: Termination, itype: IterableType
) -> Union[ClosableIterator[Any], ClosableAsyncIterator[Any]]:
    """
    Pulls `n_pulls` elements then terminates the iteration, asserting that no task (async) or thread (sync) is left behind.
    """
    if itype == "async":
        return TEST_LOOP.run_until_complete(
            apull_then_terminate_with_no_leak(s, n_pulls, termination)
        )
    assert termination.terminate is not None
    threads = set(threading.enumerate())
    it = (s.take(n_pulls) if termination.stops else s).__iter__()
    for _ in range(n_pulls):
        next(it)
    # lets the work scheduled by the last pull start in its thread
    time.sleep(SLOW / 10)
    termination.terminate(it)
    assert_no_new_threads(threads)
    it.close()  # noop
    return it


def assert_nothing_in_flight(monitored: MonitoredIdentity[Any]) -> None:
    # every started pull/call is either completed or cancelled
    assert sorted(monitored.completed + monitored.cancelled) == sorted(
        monitored.started
    )


#########
# TESTS #
#########


@pytest.mark.parametrize(
    "itype, s",
    with_itypes(
        [
            stream(range(4)),
            stream(range(4)) + range(4),
            stream(range(4)).buffer(2),
            stream(range(4)).catch(ValueError),
            stream(range(4)).do(nothing),
            stream(range(4)).do(identity, concurrency=2),
            stream(range(4)).filter(),
            stream([range(2), range(2)]).flatten(),
            stream([range(2), range(2)]).flatten(concurrency=2),
            stream(range(4)).group(2),
            stream(range(4)).group(by=nothing),
            stream(range(4)).group(within=timedelta(seconds=1)),
            stream(range(4)).group(by=nothing, within=timedelta(seconds=1)),
            stream(range(4)).map(identity),
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
        async_only_cases=[
            stream(range(4)).do(asyncio.sleep, concurrency=2),
            stream(range(4)).map(asyncio.sleep, concurrency=2),
        ],
    ),
)
@pytest.mark.parametrize("n_yields", [0, 1])
def test_close_is_idempotent(itype: IterableType, s: stream, n_yields: int) -> None:
    # the second close is done by `pull_then_terminate_with_no_leak`
    pull_then_terminate_with_no_leak(s, n_yields, CLOSE, itype)


@pytest.mark.parametrize(
    "itype, s",
    with_itypes(
        # operators spawning a puller or using an executor
        [
            stream(range(4)).buffer(2),
            stream(range(4)).do(identity, concurrency=2),
            stream([range(2), range(2)]).flatten(concurrency=2),
            stream(range(4)).group(within=timedelta(seconds=1)),
            stream(range(4)).group(by=nothing, within=timedelta(seconds=1)),
            stream(range(4)).map(identity, concurrency=2),
            stream(range(4)).map(identity, concurrency=2, as_completed=True),
        ],
        async_only_cases=[
            stream(range(4)).do(asyncio.sleep, concurrency=2),
            stream(range(4)).map(asyncio.sleep, concurrency=2),
            stream(range(4)).map(asyncio.sleep, concurrency=2, as_completed=True),
        ],
    ),
)
@pytest.mark.parametrize("n_yields", [0, 1])
def test_close_stops_iteration(itype: IterableType, s: stream, n_yields: int) -> None:
    # iterating after a close is not supported, but these operators stop instead of
    # hanging on their emptied queue, submitting to a shut down executor, or starting a puller
    it = pull_then_terminate_with_no_leak(s, n_yields, CLOSE, itype)
    with pytest.raises(stopiteration_type(itype)):
        anext_or_next(it, itype)


@pytest.mark.parametrize(
    "operate, expected",
    [
        (lambda s, slow: s.map(slow).buffer(2), [0, 1, 2, 3, 4, 5]),
        (lambda s, slow: s.map(slow, concurrency=2), [0, 1, 2, 3, 4, 5]),
        (
            lambda s, slow: s.map(slow).group(2, within=timedelta(hours=1)),
            [[0, 1], [2, 3], [4, 5]],
        ),
        (
            lambda s, slow: s.group(3)
            .map(lambda g: stream(g).map(slow))
            .flatten(concurrency=2),
            [0, 3, 1, 4, 2, 5],
        ),
    ],
)
@pytest.mark.asyncio
async def test_timeouts_do_not_end_the_iteration(
    operate: Callable[["stream[int]", Callable[[int], Awaitable[int]]], "stream[Any]"],
    expected: List[Any],
) -> None:
    monitored_identity = MonitoredIdentity[int](n_fast=0)
    it = operate(stream(range(6)), monitored_identity.async_func).__aiter__()
    elems: List[Any] = []
    n_timeouts = 0
    while True:
        try:
            elems.append(await asyncio.wait_for(it.__anext__(), timeout=SLOW / 3))
        except asyncio.TimeoutError:
            n_timeouts += 1
        except StopAsyncIteration:
            break
    # retrying after timeouts loses nothing: the in-flight work is never cancelled
    assert n_timeouts and elems == expected
    assert monitored_identity.cancelled == []
    await it.aclose()
    assert_only_current_task_is_alive()


@pytest.mark.parametrize("itype, termination", ITYPES_X_TERMINATIONS)
def test_map_termination(itype: IterableType, termination: Termination) -> None:
    src = MonitoredIntIterator()
    pull_then_terminate_with_no_leak(
        stream(src).map(identity), n_pulls=1, termination=termination, itype=itype
    )
    # nothing is in flight but the pull of a cancelled `anext`, the source is not closed
    assert (src.completed, src.cancelled, src.closed) == ([0], src.started[1:], False)


@pytest.mark.parametrize("itype, termination", ITYPES_X_TERMINATIONS)
def test_source_termination(itype: IterableType, termination: Termination) -> None:
    # the source iterator is closed if the stream created it
    iterable = MonitoredIntIterable()
    pull_then_terminate_with_no_leak(
        stream(iterable), n_pulls=1, termination=termination, itype=itype
    )
    assert [iterator.closed for iterator in iterable.iterators] == [True]

    # but not if it was provided by the client
    iterator = MonitoredIntIterator()
    pull_then_terminate_with_no_leak(
        stream(iterator), n_pulls=1, termination=termination, itype=itype
    )
    assert not iterator.closed


def await_or_call(s: "stream[Any]", itype: IterableType) -> None:
    async def await_stream() -> None:
        await s

    if itype == "async":
        TEST_LOOP.run_until_complete(await_stream())
    else:
        s()


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_owned_source_close_error_is_raised(itype: IterableType) -> None:
    it = aiter_or_iter(stream(CloseFailingMonitoredIntIterable()), itype)
    assert anext_or_next(it, itype) == 0

    # the TestError is raised on close
    with pytest.raises(TestError):
        aclose_or_close(cast(ClosableIterator[int], it), itype)

    # the TestError is raised on stop
    with pytest.raises(TestError):
        await_or_call(stream(CloseFailingMonitoredIntIterable()).take(1), itype)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_owned_source_closed_once_on_upstream_stop(itype: IterableType) -> None:
    # a stop right above the source is propagated by every operator above it, but the source is closed once
    iterable = MonitoredIntIterable()
    s = stream(iterable).take(1).map(identity).filter(lambda _: True).do(nothing)
    it = aiter_or_iter(s, itype)
    assert alist_or_list(it, itype) == [0]
    aclose_or_close(cast(ClosableIterator[int], it), itype)
    assert [iterator.n_closes for iterator in iterable.iterators] == [1]

    # a failing close is not retried: its error is raised once, then the iteration stops
    failing_iterable = CloseFailingMonitoredIntIterable()
    s = stream(failing_iterable).take(1).map(identity).filter(lambda _: True)
    it = aiter_or_iter(s, itype)
    assert anext_or_next(it, itype) == 0
    with pytest.raises(TestError):
        anext_or_next(it, itype)
    with pytest.raises(stopiteration_type(itype)):
        anext_or_next(it, itype)
    aclose_or_close(cast(ClosableIterator[int], it), itype)
    assert [iterator.n_closes for iterator in failing_iterable.iterators] == [1]


@pytest.mark.parametrize(
    "itype, operate",
    with_itypes(
        [
            lambda s: s.map(throw_func(TestError)),
            lambda s: s.map(async_throw_func(TestError), concurrency=2),
            lambda s: s.map(throw_func(TestError), concurrency=2),
            lambda s: s.buffer(2).map(throw_func(TestError)),
        ]
    ),
)
def test_await_or_call_closes_owned_source_on_error(
    itype: IterableType,
    operate: Callable[["stream[int]"], "stream[int]"],
) -> None:
    iterable = MonitoredIntIterable()
    with pytest.raises(TestError):
        await_or_call(operate(stream(iterable)), itype)
    assert not asyncio.all_tasks(TEST_LOOP)
    assert [iterator.n_closes for iterator in iterable.iterators] == [1]


@pytest.mark.asyncio
async def test_break_cancels_pending_child_tasks() -> None:
    concurrency = 4
    monitored_identity = MonitoredIdentity[int](n_fast=1)
    async for _ in stream(range(10)).do(
        monitored_identity.async_func, concurrency=concurrency
    ):
        break
    await asyncio.sleep(SLOW / 10)
    assert sorted(monitored_identity.started) == list(range(concurrency + 1))
    assert sorted(monitored_identity.completed) == [0]
    assert set(monitored_identity.cancelled) == set(range(1, concurrency + 1))


@pytest.mark.parametrize("itype, termination", ITYPES_X_TERMINATIONS)
@pytest.mark.parametrize("as_completed", [False, True])
def test_map_termination_with_concurrency(
    itype: IterableType,
    termination: Termination,
    as_completed: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monitored_identity = MonitoredIdentity[int]()
    s = stream(range(9)).map(
        monitored_identity.func(itype), concurrency=2, as_completed=as_completed
    )
    pull_then_terminate_with_no_leak(s, n_pulls=1, termination=termination, itype=itype)
    assert_nothing_in_flight(monitored_identity)
    if itype == "async":
        # the in-flight calls are cancelled, whatever ends the iteration
        assert monitored_identity.completed == [0]
        assert monitored_identity.cancelled == monitored_identity.started[1:] != []
    # no error surfaced from the futures' done callbacks
    assert not caplog.records


@pytest.mark.parametrize("itype, termination", ITYPES_X_TERMINATIONS)
def test_buffer_cancel(itype: IterableType, termination: Termination) -> None:
    src = MonitoredIntIterator()
    pull_then_terminate_with_no_leak(
        stream(src).buffer(1), n_pulls=1, termination=termination, itype=itype
    )
    # the in-flight pull is cancelled (async) or awaited (sync), the source is not closed
    assert_nothing_in_flight(src)
    assert (src.started, src.closed) == ([0, 1], False)
    if itype == "async":
        assert src.cancelled == [1]


@pytest.mark.parametrize("itype, termination", ITYPES_X_TERMINATIONS)
def test_group_cancel(itype: IterableType, termination: Termination) -> None:
    src = MonitoredIntIterator()
    pull_then_terminate_with_no_leak(
        stream(src).group(within=timedelta(seconds=SLOW / 5)),
        n_pulls=1,
        termination=termination,
        itype=itype,
    )
    # the in-flight pull is cancelled (async) or awaited (sync), the source is not closed
    assert_nothing_in_flight(src)
    assert (src.started, src.closed) == ([0, 1], False)
    if itype == "async":
        assert src.cancelled == [1]


@pytest.mark.parametrize("itype, termination", ITYPES_X_TERMINATIONS)
def test_flatten_provided_inner_iter_cancel(
    itype: IterableType, termination: Termination
) -> None:
    a, b, c = MonitoredIntIterator(), MonitoredIntIterator(), MonitoredIntIterator()
    s = stream([a, b, c]).flatten(concurrency=2)
    pull_then_terminate_with_no_leak(s, n_pulls=2, termination=termination, itype=itype)
    # the in-flight pulls are cancelled (async) or awaited (sync), the inner iterators are not closed
    for src in (a, b):
        assert_nothing_in_flight(src)
        assert (src.started, src.closed) == ([0, 1], False)
        if itype == "async":
            assert src.cancelled == [1]
    assert c.started == []


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
@pytest.mark.parametrize("concurrency", [1, 2])
def test_flatten_owned_inner_iter_close_error_is_silent(
    itype: IterableType, concurrency: int
) -> None:
    # the owned inner iters closure is done in best effort, errors are silenced
    iterable = CloseFailingMonitoredIntIterable()
    # on close:
    s = stream([iterable]).flatten(concurrency=concurrency)
    it = aiter_or_iter(s, itype)
    assert anext_or_next(it, itype) == 0
    aclose_or_close(cast(ClosableIterator[int], it), itype)
    assert iterable.iterators[-1].closed is True

    # on stop:
    it = aiter_or_iter(s.take(1), itype)
    assert anext_or_next(it, itype) == 0
    with pytest.raises(stopiteration_type(itype)):
        anext_or_next(it, itype)
    assert iterable.iterators[-1].closed is True


@pytest.mark.parametrize("itype, termination", ITYPES_X_TERMINATIONS)
@pytest.mark.parametrize("concurrency", [1, 2])
def test_flatten_owned_inner_iter_cancel(
    itype: IterableType, termination: Termination, concurrency: int
) -> None:
    src = MonitoredIntIterator()
    s = stream([stream(src).buffer(1)]).flatten(concurrency=concurrency)
    pull_then_terminate_with_no_leak(s, n_pulls=1, termination=termination, itype=itype)
    # closing the owned inner stream iter cancels (async) or awaits (sync) its in-flight pull,
    # the provided `src` is not closed
    assert_nothing_in_flight(src)
    assert not src.closed
    if itype == "async":
        assert (src.started, src.cancelled) == ([0, 1], [1])
    # a sync concurrent flatten awaits its in-flight `next` on the inner stream iter,
    # which frees a slot of the inner `.buffer`, so the source may get pulled once more


@pytest.mark.asyncio
async def test_flatten_cancel_as_owned_inner_pull_completes() -> None:
    consumer: asyncio.Task

    src = MonitoredIntIterable()
    self_cancelling = (
        stream(src)
        # Triggers a cancellation of `await s` landing just after the pull completed:
        # the cancellation itself closes nothing, `src` gets closed by the
        # `aclosing` of `stream.__await__` (through the concurrent flatten's
        # `aclose`, which closes its owned inner iterators).
        .do(lambda _: asyncio.get_running_loop().call_soon(consumer.cancel))
    )
    consumer = asyncio.ensure_future(stream([self_cancelling]).flatten(concurrency=2))

    with pytest.raises(asyncio.CancelledError):
        await consumer
    assert_only_current_task_is_alive()
    assert [iterator.closed for iterator in src.iterators] == [True]


@pytest.mark.parametrize("itype, termination", ITYPES_X_TERMINATIONS)
@pytest.mark.parametrize(
    "concurrency, src_getters",
    [
        (1, [MonitoredIntIterable]),
        (1, [MonitoredIntIterator]),
        (2, [MonitoredIntIterable, MonitoredIntIterator, MonitoredIntIterable]),
        (3, [MonitoredIntIterable, MonitoredIntIterator, MonitoredIntIterable]),
        (3, [lambda: [0], MonitoredIntIterable, MonitoredIntIterator]),
    ],
)
def test_flatten_both_owned_and_provided_inner_iters(
    itype: IterableType,
    termination: Termination,
    concurrency: int,
    src_getters: List[Callable[[], Iterable[int]]],
) -> None:
    src = [get_src() for get_src in src_getters]
    s = stream(src).flatten(concurrency=concurrency)
    pull_then_terminate_with_no_leak(s, n_pulls=2, termination=termination, itype=itype)
    # in-flight owned iterators are closed
    assert all(
        iterator.closed
        for iterable in src[:concurrency]
        if isinstance(iterable, MonitoredIntIterable)
        for iterator in iterable.iterators
    )
    # in-flight client-provided iterators are not
    assert not any(
        iterable.closed
        for iterable in src[:concurrency]
        if isinstance(iterable, MonitoredIntIterator)
    )


@pytest.mark.parametrize("itype, termination", ITYPES_X_TERMINATIONS)
def test_add_close(itype: IterableType, termination: Termination) -> None:
    pull_then_terminate_with_no_leak(
        stream(MonitoredIntIterator()).buffer(1) + [9],
        n_pulls=1,
        termination=termination,
        itype=itype,
    )


@pytest.mark.parametrize("itype, termination", ITYPES_X_TERMINATIONS)
def test_observe_termination_has_no_leak(
    itype: IterableType, termination: Termination
) -> None:
    monitored_identity = MonitoredIdentity[Any](n_fast=0)
    s = stream(MonitoredIntIterator()).observe(
        every=timedelta(seconds=SLOW),
        do=monitored_identity.func(itype),
    )
    pull_then_terminate_with_no_leak(s, n_pulls=1, termination=termination, itype=itype)
    # the in-flight observation is awaited, whatever ends the iteration
    assert monitored_identity.completed == monitored_identity.started != []


@pytest.mark.parametrize("itype, termination", ITYPES_X_TERMINATIONS)
def test_chain_cancellation(itype: IterableType, termination: Termination) -> None:
    src = MonitoredIntIterator(n_fast=20)
    pull_then_terminate_with_no_leak(
        chain(src, itype), n_pulls=1, termination=termination, itype=itype
    )
    # the termination reaches the in-flight pull through every operator
    assert_nothing_in_flight(src)
    assert not src.closed
    if itype == "async":
        assert len(src.cancelled) == 1


@pytest.mark.parametrize("itype, termination", ITYPES_X_TERMINATIONS)
@pytest.mark.parametrize("n_pulls", [0, 1])
def test_chain_with_owned_source_termination(
    itype: IterableType, termination: Termination, n_pulls: int
) -> None:
    iterable = MonitoredIntIterable(n_fast=20)
    pull_then_terminate_with_no_leak(
        chain(iterable, itype), n_pulls=n_pulls, termination=termination, itype=itype
    )
    # the closure reaches the source through every operator, even the ones not started yet,
    # and closes it once its in-flight pull is done
    assert [
        (iterator.n_closes, iterator.n_pulls_in_flight_at_close)
        for iterator in iterable.iterators
    ] == [(1, 0)]


@pytest.mark.asyncio
async def test_cancel_during_cancel() -> None:
    class SlowToCancel(MonitoredIntIterator):
        async def __anext__(self) -> int:
            try:
                return await super().__anext__()
            except asyncio.CancelledError:
                await asyncio.sleep(1)
                raise

    src = SlowToCancel()
    closing = asyncio.ensure_future(
        apull_then_terminate_with_no_leak(
            stream(src).buffer(1), n_pulls=1, termination=CLOSE
        )
    )
    await asyncio.sleep(SLOW / 10)
    # the close waits for the source to honor the cancellation
    assert src.cancelled == [1] and not closing.done()
    # a second cancellation is not swallowed
    closing.cancel()
    audit_closing = await audit_awaitable(closing, assert_raises=asyncio.CancelledError)
    assert audit_closing.duration < SLOW / 10
    # the interrupted close leaves the source finishing its cancellation
    await asyncio.wait(asyncio.all_tasks() - {asyncio.current_task()})
    assert_only_current_task_is_alive()


###########
# THREADS #
###########


async def max_event_loop_stall(coro: Awaitable[T]) -> float:
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
@pytest.mark.parametrize("itype, termination", ITYPES_X_TERMINATIONS)
def test_close_executor_backed(
    get_stream: Callable[[MonitoredIdentity[int]], "stream[int]"],
    itype: IterableType,
    termination: Termination,
) -> None:
    monitored_identity = MonitoredIdentity[int](n_fast=0)
    threads = set(threading.enumerate())
    s = get_stream(monitored_identity)
    if itype == "async":
        stall = TEST_LOOP.run_until_complete(
            max_event_loop_stall(
                apull_then_terminate_with_no_leak(s, n_pulls=1, termination=termination)
            )
        )
        # the in-flight calls are awaited without blocking the event loop
        assert stall < SLOW / 5
    else:
        pull_then_terminate_with_no_leak(
            s, n_pulls=1, termination=termination, itype=itype
        )
    # the in-flight calls are awaited
    assert sorted(monitored_identity.completed) == sorted(monitored_identity.started)
    # threads are joined
    assert_no_new_threads(threads)


@pytest.mark.parametrize("as_completed", [False, True])
@pytest.mark.asyncio
async def test_cancel_executor_backed_map_cancels_queued_calls(
    as_completed: bool,
) -> None:
    monitored_identity = MonitoredIdentity[int](n_fast=0)
    s = stream(range(9)).map(
        monitored_identity.sync_func, concurrency=2, as_completed=as_completed
    )
    # 2 calls running in the executor when timeout occurs
    # 1 queued call not started (no worker available yet)
    await apull_then_terminate_with_no_leak(
        s, n_pulls=0, termination=NEXT_UNTIL_TIMEOUT_THEN_CLOSE
    )
    # running calls are awaited, queued one never started
    assert sorted(monitored_identity.started) == [0, 1]
    assert sorted(monitored_identity.completed) == [0, 1]


@pytest.mark.asyncio
async def test_close_executor_backed_map_is_interruptible() -> None:
    monitored_identity = MonitoredIdentity[int]()
    threads = set(threading.enumerate())
    it = (
        stream(MonitoredIntIterable())
        .map(monitored_identity.sync_func, concurrency=2)
        .__aiter__()
    )
    assert await it.__anext__() == 0
    await asyncio.sleep(SLOW / 10)
    closing = asyncio.ensure_future(it.aclose())
    await asyncio.sleep(0)
    # cancelling the close interrupts it right away
    closing.cancel()
    audit_closing = await audit_awaitable(closing, assert_raises=asyncio.CancelledError)
    assert audit_closing.duration < SLOW / 10
    # a later close waits for the in-flight calls and joins the threads
    await it.aclose()
    assert sorted(monitored_identity.completed) == sorted(monitored_identity.started)
    assert_no_new_threads(threads)
