import asyncio
from datetime import timedelta
from functools import partial
from typing import Callable, List
import pytest
from streamable import stream
from streamable._tools._async import anext
from tests.tools.closing import aclosing
from tests.tools.func import nothing
from tests.tools.timing import time_coroutine


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

    duration, _ = await time_coroutine(pull_1_and_close)
    assert duration < 0.01


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

    duration, _ = await time_coroutine(pull_1_and_close)
    assert duration == pytest.approx(2, rel=0.01)


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

    duration, _ = await time_coroutine(pull_1_and_close)
    assert duration < 0.01
