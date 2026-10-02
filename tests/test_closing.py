import asyncio
from collections import deque
from datetime import timedelta
from functools import partial
from typing import Any, AsyncIterator, Callable, Deque, List

import pytest

from streamable import star, stream
from streamable._tools._async import anext
from streamable._tools._context import aclosing
from tests.tools.error import TestBaseError, TestError
from tests.tools.func import (
    async_identity,
    audit_async_func,
    identity,
    nothing,
    throw_func,
)
from tests.tools.iter import alist


def through_all_operators(src: AsyncIterator[int]) -> stream[int]:
    return (
        stream(src)
        .buffer()
        .catch(TestError)
        .do(nothing)
        .filter(lambda _: True)
        .group(2)
        .flatten()
        .group(2, by=lambda n: n % 2)
        .map(star(lambda _, group: group))
        .flatten(concurrency=2)
        .group(2, within=timedelta(hours=1))
        .map(stream)
        .flatten(concurrency=2)
        .group(2, by=lambda n: n % 2, within=timedelta(hours=1))
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


async def logged_sleep(
    n: int, started: List[int], cancelled: List[int], completed: List[int]
) -> None:
    started.append(n)
    try:
        await asyncio.sleep(n / 10)
    except asyncio.CancelledError:
        cancelled.append(n)
        raise
    completed.append(n)


@pytest.mark.parametrize("n_yields, concurrency", [(0, 2), (1, 2), (2, 2), (3, 2)])
@pytest.mark.parametrize("as_completed", [False, True])
@pytest.mark.asyncio
async def test_closing_concurrent_map(
    n_yields: int,
    concurrency: int,
    as_completed: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    started: List[int] = []
    cancelled: List[int] = []
    completed: List[int] = []
    yielded: List[int] = []
    async with aclosing(
        stream(range(10))
        .do(
            partial(
                logged_sleep, started=started, cancelled=cancelled, completed=completed
            ),
            concurrency=concurrency,
            as_completed=as_completed,
        )
        .do(nothing)
        .__aiter__()
    ) as it:
        for _ in range(n_yields):
            yielded.append(await anext(it))
        await asyncio.sleep(0)

    assert yielded == list(range(n_yields))

    if n_yields:
        assert started == list(range(0, len(yielded) + concurrency))
        assert completed == yielded
        assert cancelled == list(range(len(yielded), len(yielded) + concurrency))
    else:
        assert started == []
        assert completed == []
        assert cancelled == []
    # no error raised in futures' done callbacks
    assert not caplog.records


@pytest.mark.parametrize("n_yields, concurrency", [(0, 2), (1, 2), (2, 2), (3, 2)])
@pytest.mark.asyncio
async def test_closing_concurrent_flatten(
    n_yields: int,
    concurrency: int,
) -> None:
    started: List[int] = []
    cancelled: List[int] = []
    completed: List[int] = []
    yielded: List[int] = []

    async def get_src(start: int, end: int, step: int) -> AsyncIterator[int]:
        for i in range(start, end, step):
            yield i

    src1 = get_src(0, 10, 2)
    src2 = get_src(1, 10, 2)
    async with aclosing(
        stream(
            [
                stream(src).do(
                    partial(
                        logged_sleep,
                        started=started,
                        cancelled=cancelled,
                        completed=completed,
                    )
                )
                for src in (src1, src2)
            ]
        )
        .flatten(concurrency=concurrency)
        .do(nothing)
        .__aiter__()
    ) as it:
        for _ in range(n_yields):
            yielded.append(await anext(it))
        await asyncio.sleep(0)

    assert yielded == list(range(n_yields))

    if n_yields:
        assert started == list(range(0, len(yielded) + concurrency))
        assert completed == started
        assert cancelled == []
    else:
        assert started == []
        assert completed == []
        assert cancelled == []


@pytest.mark.asyncio
async def test_closing_group_within() -> None:
    async def get_src() -> AsyncIterator[int]:
        for n in range(10):
            await asyncio.sleep(n / 10)
            yield n

    src = get_src()

    async def pull_1_and_close() -> None:
        async with aclosing(
            stream(src).group(within=timedelta(seconds=0.2)).do(nothing).__aiter__()
        ) as it:
            assert await anext(it) == [0, 1]

    audit = await audit_async_func(pull_1_and_close)
    assert audit.avg_duration == pytest.approx(0.3, abs=0.01)
    assert audit.avg_leftover_tasks == 0
    # elem 2 is lost because its pulling started before the aclose
    assert await anext(src) == 3


@pytest.mark.parametrize("do_duration", [0, 1])
@pytest.mark.asyncio
async def test_closing_observe_every(do_duration: float) -> None:
    async def do(_: object) -> None:
        await asyncio.sleep(do_duration)

    it = stream(range(10)).observe(every=timedelta(seconds=0.5), do=do).__aiter__()
    await anext(it)
    # the observer task is either sleeping or inside `do`
    await asyncio.sleep(0.1)

    async def close() -> None:
        await it.aclose()

    audit = await audit_async_func(close)
    # the observer task is cancelled rather than awaited
    assert audit.avg_duration < 0.01
    assert audit.avg_leftover_tasks == 0


@pytest.mark.asyncio
async def test_closing_observe_on_stop_background_task_not_leaked() -> None:
    async def exhaust() -> None:
        await stream(range(3)).do(asyncio.sleep).observe(every=timedelta(seconds=2.5))

    audit = await audit_async_func(exhaust)
    assert audit.avg_duration == pytest.approx(3, rel=0.01)
    assert audit.avg_leftover_tasks == 0


@pytest.mark.parametrize(
    "s",
    [
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
async def test_closing_then_anext(s: stream, n_yields: int) -> None:
    it = s.__aiter__()
    for _ in range(n_yields):
        await anext(it)
    await it.aclose()
    with pytest.raises(StopAsyncIteration):
        await anext(it)


@pytest.mark.parametrize(
    "get_stream",
    [
        lambda: stream(range(10)).do(asyncio.sleep, concurrency=2).take(1),
        lambda: stream(range(10))
        .do(asyncio.sleep, concurrency=2)
        .take(until=lambda n: n == 0),
        lambda: stream(range(10))
        .do(throw_func(TestError), concurrency=2)
        .catch(TestError, stop=True),
        lambda: stream(
            [
                stream(range(0, 10, 2)).do(asyncio.sleep),
                stream(range(1, 10, 2)).do(asyncio.sleep),
            ]
        )
        .flatten(concurrency=2)
        .take(1),
        lambda: stream(
            [
                stream(range(0, 10, 2)).do(throw_func(TestError)),
                stream(range(1, 10, 2)).do(asyncio.sleep),
            ]
        )
        .flatten(concurrency=2)
        .catch(TestError, stop=True),
        lambda: stream(range(10)).do(asyncio.sleep).buffer(2).take(1),
        lambda: stream(range(10))
        .do(throw_func(TestError))
        .buffer(2)
        .catch(TestError, stop=True),
        lambda: stream(range(10))
        .do(asyncio.sleep)
        .group(1, within=timedelta(hours=1))
        .take(1),
        lambda: stream(range(10))
        .do(throw_func(TestError))
        .group(1, within=timedelta(hours=1))
        .catch(TestError, stop=True),
        lambda: stream(range(10)).observe(every=timedelta(hours=1), do=nothing).take(1),
        lambda: stream(range(10))
        .do(throw_func(TestError))
        .observe(every=timedelta(hours=1), do=nothing)
        .catch(TestError, stop=True),
    ],
)
@pytest.mark.asyncio
async def test_closing_on_early_stop(
    get_stream: Callable[[], stream[Any]],
) -> None:
    it = get_stream().__aiter__()

    async def exhaust_it() -> None:
        while True:
            try:
                await anext(it)
            except StopAsyncIteration:
                break
        await asyncio.sleep(0)

    audit = await audit_async_func(exhaust_it)
    assert audit.avg_leftover_tasks == 0


@pytest.mark.asyncio
async def test_closing_through_all_operators_neither_closes_source_nor_cancels_its_pulling() -> (
    None
):
    src_cancelled = False
    src_closed = False

    async def get_src() -> AsyncIterator[int]:
        nonlocal src_cancelled, src_closed
        try:
            for n in range(100):
                yield n
            # the `.buffer`'s pull of 100 is pending when closing
            await asyncio.sleep(0.2)
            yield 100
            yield 101
        except asyncio.CancelledError:
            src_cancelled = True
            raise
        except GeneratorExit:
            src_closed = True
            raise

    src = get_src()
    it = through_all_operators(src).__aiter__()
    await anext(it)

    async def close() -> None:
        await it.aclose()

    audit = await audit_async_func(close)
    # the pending pull is awaited rather than cancelled
    assert audit.avg_duration == pytest.approx(0.2, abs=0.05)
    assert audit.avg_leftover_tasks == 0
    assert not src_cancelled
    assert not src_closed
    # 100 got pulled by the `.buffer` and dropped, the source resumes after it
    assert await anext(src) == 101


################
# cancellation #
################


@pytest.mark.parametrize(
    "get_stream",
    [
        lambda src: stream(src),
        lambda src: stream(src).do(nothing),
        lambda src: stream(src).map(async_identity, concurrency=2),
        lambda src: stream(src).buffer(2),
        lambda src: stream(src).group(1, within=timedelta(hours=1)),
    ],
)
@pytest.mark.parametrize("timeout", [0.0001, 0.5])
@pytest.mark.asyncio
async def test_closing_cancel_propagates_to_source_pulling(
    get_stream: Callable[[Any], stream[object]], timeout: float
) -> None:
    cancelled = False

    async def get_stuck_src() -> AsyncIterator[int]:
        nonlocal cancelled
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            cancelled = True
            raise
        yield 0

    src = get_stuck_src()
    s = get_stream(src)
    it = s.__aiter__()

    async def cancel_anext() -> None:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(anext(it), timeout=timeout)

    audit = await audit_async_func(cancel_anext)

    # the timeout is honored
    assert audit.avg_duration == pytest.approx(timeout, abs=0.01)
    # no tasks left over after the cancellation
    assert audit.avg_leftover_tasks == 0

    # the cancellation reached the source
    assert cancelled
    # the source iterator is stopped after the cancellation
    with pytest.raises(StopAsyncIteration):
        await anext(src)


# the iteration either times out or gets interrupted by a `BaseException` raised by the first inner iter
@pytest.mark.parametrize("raise_base_exception", [False, True])
@pytest.mark.parametrize("concurrency", [1, 2])
@pytest.mark.parametrize("timeout", [0.0001, 0.5])
@pytest.mark.asyncio
async def test_closing_cancel_propagates_to_flattened_iterators(
    concurrency: int, timeout: float, raise_base_exception: bool
) -> None:
    n_inner_iters = 4

    cancellation_flags: List[Deque[object]] = [deque() for _ in range(n_inner_iters)]

    async def get_stuck_iter(
        cancellation_flag: Deque[object], raises: bool
    ) -> AsyncIterator[object]:
        try:
            await asyncio.sleep(timeout if raises else 1)
        except asyncio.CancelledError:
            cancellation_flag.append(None)
            raise
        if raises:
            raise TestBaseError()
        yield 0

    inner_iters = [
        get_stuck_iter(flag, raises=raise_base_exception and i == 0)
        for i, flag in enumerate(cancellation_flags)
    ]

    async def get_src() -> AsyncIterator[AsyncIterator[object]]:
        for inner_iter in inner_iters:
            yield inner_iter

    src = get_src()
    s = stream(src).flatten(concurrency=concurrency)
    it = s.__aiter__()

    async def cancel_anext() -> None:
        if raise_base_exception:
            with pytest.raises(TestBaseError):
                await anext(it)
        else:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(anext(it), timeout=timeout)

    audit = await audit_async_func(cancel_anext)

    # the timeout is honored
    assert audit.avg_duration == pytest.approx(timeout, abs=0.01)

    # no tasks left over after the cancellation
    assert audit.avg_leftover_tasks == 0

    # the cancellation reached the inner iters being flattened (except the one that raised)
    assert all(cancellation_flags[int(raise_base_exception) : concurrency])
    assert all(not flag for flag in cancellation_flags[concurrency:])

    # the inner iters being flattened are stopped
    for inner_iter in inner_iters[:concurrency]:
        with pytest.raises(StopAsyncIteration):
            await anext(inner_iter)

    # the source iterator is not stopped
    assert await anext(src) is inner_iters[concurrency]


# the `.do` either times out or gets interrupted by a `BaseException` raised by its function
@pytest.mark.parametrize("raise_base_exception", [False, True])
@pytest.mark.parametrize("as_completed", [False, True])
@pytest.mark.asyncio
async def test_closing_cancel_propagates_from_downstream_to_upstream_operator(
    as_completed: bool, raise_base_exception: bool, request: pytest.FixtureRequest
) -> None:
    if as_completed and raise_base_exception:
        # the FDFO done-callback raises the `BaseException` instead of queuing it
        request.applymarker(
            pytest.mark.xfail(strict=True, reason="`BaseException` not propagated")
        )

    async def get_src() -> AsyncIterator[int]:
        for duration in (0, 5, 5, 5):
            yield duration
        # buffer's pull of the next element stays pending
        await asyncio.sleep(1)
        yield 0

    async def sleep(duration: int) -> None:
        if raise_base_exception and duration == 5:
            await asyncio.sleep(0.1)
            raise TestBaseError()
        await asyncio.sleep(duration)

    # cancellation lands on `.do` waiting for a pending `sleep(5)`, its cleanup closes the `.buffer`
    it = (
        stream(get_src())
        .buffer(10)
        .do(sleep, concurrency=2, as_completed=as_completed)
        .__aiter__()
    )
    await anext(it)

    async def cancel_anext() -> None:
        if raise_base_exception:
            with pytest.raises(TestBaseError):
                await asyncio.wait_for(anext(it), timeout=1)
        else:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(anext(it), timeout=0.1)

    audit = await audit_async_func(cancel_anext)

    # the timeout is honored: the `.buffer` cancels its pending pull instead of waiting for it
    assert audit.avg_duration == pytest.approx(0.1, abs=0.01)


@pytest.mark.asyncio
async def test_closing_in_cancelled_task_cancels_upstream_pulling() -> None:
    async def get_src() -> AsyncIterator[int]:
        yield 0
        # the buffer's pull of the next element stays pending
        await asyncio.sleep(1)
        yield 1

    it = stream(get_src()).buffer(2).__aiter__()

    async def cancel_while_holding_it() -> None:
        async def hold_it() -> None:
            async with aclosing(it):
                await anext(it)
                # the cancellation lands here: the `.buffer` only sees the `.aclose` from `aclosing`
                await asyncio.sleep(3600)

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(hold_it(), timeout=0.1)

    audit = await audit_async_func(cancel_while_holding_it)

    # the timeout is honored: the `.buffer` cancels its pending pull instead of waiting for it
    assert audit.avg_duration == pytest.approx(0.1, abs=0.01)


@pytest.mark.asyncio
async def test_closing_cancel_recovered_from_does_not_affect_later_closes() -> None:
    cancelled_srcs: List[str] = []

    async def get_src(name: str) -> AsyncIterator[int]:
        try:
            yield 0
            await asyncio.sleep(0.1)
            yield 1
        except asyncio.CancelledError:
            cancelled_srcs.append(name)
            raise

    # a timeout that the task recovers from
    it = stream(get_src("timed out")).buffer(2).__aiter__()
    await anext(it)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(anext(it), timeout=0.01)
    assert cancelled_srcs == ["timed out"]

    # later in the same task, an early stop waits for the pending pull instead of cancelling it
    assert await alist(stream(get_src("stopped")).buffer(2).take(1)) == [0]
    assert cancelled_srcs == ["timed out"]


@pytest.mark.asyncio
async def test_closing_cancel_during_cancelling_aclose_propagates() -> None:
    async def get_slowly_cancellable_src() -> AsyncIterator[int]:
        yield 0
        try:
            await asyncio.sleep(8)
        except asyncio.CancelledError:
            await asyncio.sleep(0.2)
            raise

    async def aclose_from_cancellation_context() -> None:
        it = stream(get_slowly_cancellable_src()).buffer(2).do(nothing).__aiter__()
        await anext(it)
        await asyncio.sleep(0)  # the buffer's pull of the next element is pending
        try:
            raise asyncio.CancelledError()
        except asyncio.CancelledError:
            # close in the context of a cancellation tells the operator to eagerly close, cancelling pending work
            await it.aclose()

    task = asyncio.create_task(aclose_from_cancellation_context())
    await asyncio.sleep(0.05)
    # cancelled while the `.aclose` waits for the source to honor the cancellation
    task.cancel()

    async def wait_for_task() -> None:
        with pytest.raises(asyncio.CancelledError):
            await task

    audit = await audit_async_func(wait_for_task)
    assert audit.avg_duration < 0.01
    assert audit.avg_leftover_tasks == 0


@pytest.mark.asyncio
async def test_closing_cancel_propagates_through_all_operators_to_source_pulling() -> (
    None
):
    src_cancelled = False

    async def get_stuck_src() -> AsyncIterator[int]:
        nonlocal src_cancelled
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            src_cancelled = True
            raise
        yield 0

    it = through_all_operators(get_stuck_src()).__aiter__()

    async def cancel_anext() -> None:
        async with aclosing(it):
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(anext(it), timeout=0.1)

    audit = await audit_async_func(cancel_anext)
    assert audit.avg_duration == pytest.approx(0.1, abs=0.01)
    assert audit.avg_leftover_tasks == 0
    assert src_cancelled


@pytest.mark.parametrize(
    "get_stream",
    [
        lambda src: stream(src).buffer(1),
        # the group gets yielded on `within` timeout while the next element's pull is pending
        lambda src: stream(src).group(100, within=timedelta(seconds=0.01)),
    ],
)
@pytest.mark.asyncio
async def test_closing_cancel_of_background_task_terminates_it(
    get_stream: Callable[[Any], stream[object]],
) -> None:
    async def get_stuck_src() -> AsyncIterator[int]:
        yield 0
        await asyncio.sleep(1)
        yield 1

    it = get_stream(get_stuck_src()).__aiter__()
    await anext(it)
    await asyncio.sleep(0)  # the background task's pull of the next element is pending

    # e.g. a shutdown that cancels all tasks then waits for them
    background_tasks = asyncio.all_tasks() - {asyncio.current_task()}
    for task in background_tasks:
        task.cancel()

    async def wait_for_tasks() -> None:
        await asyncio.wait_for(
            asyncio.gather(*background_tasks, return_exceptions=True), timeout=0.1
        )

    audit = await audit_async_func(wait_for_tasks)
    assert audit.avg_duration < 0.01
    assert audit.avg_leftover_tasks == 0
