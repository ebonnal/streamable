import asyncio
from datetime import timedelta
from functools import partial
from typing import Any, Callable, List
import pytest
from streamable import stream
from streamable._tools._async import anext
from tests.tools.closing import aclosing
from tests.tools.error import TestError
from tests.tools.func import nothing, throw_func
from tests.tools.func import audit_async_func


async def logged_sleep(
    n: int, started: List[int], cancelled: List[int], completed: List[int]
) -> None:
    started.append(n)
    try:
        await asyncio.sleep(n)
    except asyncio.CancelledError:
        cancelled.append(n)
        raise
    completed.append(n)


@pytest.mark.parametrize("n_yields, concurrency", [(0, 2), (1, 2), (2, 2), (3, 2)])
@pytest.mark.parametrize(
    "get_stream",
    [
        lambda concurrency, started, cancelled, completed: stream(range(10)).do(
            partial(
                logged_sleep, started=started, cancelled=cancelled, completed=completed
            ),
            concurrency=concurrency,
        ),
        lambda concurrency, started, cancelled, completed: stream(
            [
                stream(range(0, 10, 2)).do(
                    partial(
                        logged_sleep,
                        started=started,
                        cancelled=cancelled,
                        completed=completed,
                    )
                ),
                stream(range(1, 10, 2)).do(
                    partial(
                        logged_sleep,
                        started=started,
                        cancelled=cancelled,
                        completed=completed,
                    )
                ),
            ]
        ).flatten(concurrency=concurrency),
    ],
)
@pytest.mark.asyncio
async def test_aclose_with_concurrent_operations(
    n_yields: int,
    concurrency: int,
    get_stream: Callable[[int, List[int], List[int], List[int]], stream[int]],
) -> None:
    started: List[int] = []
    cancelled: List[int] = []
    completed: List[int] = []
    yielded: List[int] = []
    async with aclosing(
        get_stream(concurrency, started, cancelled, completed).do(nothing).__aiter__()
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


@pytest.mark.asyncio
async def test_aclose_buffer() -> None:
    async def pull_1_and_close() -> None:
        async with aclosing(
            stream(range(10)).do(asyncio.sleep).buffer(2).do(nothing).__aiter__()
        ) as it:
            assert await anext(it) == 0

    audit = await audit_async_func(pull_1_and_close)
    assert audit.avg_duration < 0.01
    assert audit.avg_leftover_tasks == 0


@pytest.mark.asyncio
async def test_aclose_group_within() -> None:
    async def pull_1_and_close() -> None:
        async with aclosing(
            stream(range(10))
            .do(asyncio.sleep)
            .group(within=timedelta(seconds=2))
            .do(nothing)
            .__aiter__()
        ) as it:
            assert await anext(it) == [0, 1]

    audit = await audit_async_func(pull_1_and_close)
    assert audit.avg_duration == pytest.approx(2, rel=0.01)
    assert audit.avg_leftover_tasks == 0


@pytest.mark.asyncio
async def test_aclose_observe_every() -> None:
    async def pull_1_and_close() -> None:
        async with aclosing(
            stream(range(10))
            .do(asyncio.sleep)
            .observe(every=timedelta(seconds=0.99))
            .do(nothing)
            .__aiter__()
        ) as it:
            assert await anext(it) == 0

    audit = await audit_async_func(pull_1_and_close)
    assert audit.avg_duration < 0.01
    assert audit.avg_leftover_tasks == 0


@pytest.mark.asyncio
async def test_aclose_observe_on_stop() -> None:
    async def exhaust() -> None:
        await stream(range(3)).do(asyncio.sleep).observe(every=timedelta(seconds=2.5))

    audit = await audit_async_func(exhaust)
    assert audit.avg_duration == pytest.approx(3, rel=0.01)
    assert audit.avg_leftover_tasks == 0


@pytest.mark.parametrize(
    "s",
    [
        stream(range(4)).map(lambda n: n),
        stream(range(4)).map(asyncio.sleep, concurrency=2),
        stream(range(4)).map(lambda n: n, concurrency=2),
        stream(range(4)).do(nothing),
        stream(range(4)).do(asyncio.sleep, concurrency=2),
        stream(range(4)).filter(),
        stream(range(4)).take(2),
        stream(range(4)).take(until=lambda n: n == 2),
        stream(range(4)).skip(1),
        stream(range(4)).skip(until=lambda n: n == 2),
        stream(range(4)).catch(ValueError),
        stream(range(4)).throttle(2, per=timedelta(seconds=1)),
        stream(range(4)).buffer(2),
        stream(range(4)).group(2),
        stream(range(4)).group(by=lambda n: n % 2),
        stream(range(4)).group(within=timedelta(seconds=1)),
        stream(range(4)).group(by=lambda n: n % 2, within=timedelta(seconds=1)),
        stream([range(2), range(2)]).flatten(),
        stream([range(2), range(2)]).flatten(concurrency=2),
        stream(range(4)).observe(do=nothing),
        stream(range(4)).observe(every=2, do=nothing),
        stream(range(4)).observe(every=timedelta(seconds=1), do=nothing),
        stream(range(4)) + range(4),
    ],
)
@pytest.mark.parametrize("n_yields", [0, 1])
@pytest.mark.asyncio
async def test_aclose_then_anext(s: stream, n_yields: int) -> None:
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
async def test_aclose_called_on_early_exhaustion_via_take_or_stop_on_catch(
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
    await it.aclose()
