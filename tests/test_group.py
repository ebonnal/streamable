import asyncio
import datetime
import time
from typing import Any, AsyncIterable, Callable, Iterable, List, Optional
from datetime import timedelta
import pytest

from streamable import stream
from streamable._tools._func import asyncify
from tests.tools.func import (
    SLOW_IDENTITY_DURATION,
    identity,
    async_slow_identity,
    slow_identity,
)
from tests.tools.iter import (
    ITERABLE_TYPES,
    IterableType,
    aiter_or_iter,
    alist_or_list,
)
from tests.tools.source import INTEGERS, ints


@pytest.mark.parametrize(
    "within, repr_pattern",
    [
        (datetime.timedelta(), r"datetime\.timedelta\(0\)"),
        (datetime.timedelta(days=-1), r"datetime\.timedelta\(days=-1\)"),
    ],
)
def test_group_raises_on_non_positive(
    within: datetime.timedelta, repr_pattern: str
) -> None:
    with pytest.raises(
        ValueError,
        match=rf"`within` must be a positive timedelta but got: {repr_pattern}",
    ):
        stream([1]).group(up_to=100, within=within)


@pytest.mark.parametrize("up_to", [-1, 0])
def test_group_raises_on_non_positive_up_to(up_to: int) -> None:
    with pytest.raises(ValueError, match=f"`up_to` must be >= 1 but got: {up_to}"):
        stream([]).group(up_to=up_to)


@pytest.mark.parametrize(
    "upstream, up_to, by, within, expected",
    [
        # empty upstream
        (stream([]), 4, None, None, []),
        # no up_to
        (stream(range(10)), None, None, None, [list(range(10))]),
        # all groups full
        (stream(range(10)), 2, None, None, [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]]),
        # incomplete last group
        (stream(range(10)), 4, None, None, [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]]),
        # mono-element last group
        (stream(range(10)), 3, None, None, [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]),
        # yield incomplete group on upstream exception
        (
            stream("01-23456789").map(int),
            3,
            None,
            None,
            [[0, 1], [2, 3, 4], [5, 6, 7], [8, 9]],
        ),
        # by key
        (
            stream(range(10)),
            2,
            lambda n: n % 2,
            None,
            [(0, [0, 2]), (1, [1, 3]), (0, [4, 6]), (1, [5, 7]), (0, [8]), (1, [9])],
        ),
        # by with upstream exception, yield all pending groups FIFO
        (
            stream("01-23456789").map(int),
            2,
            lambda n: n % 2,
            None,
            [(0, [0]), (1, [1]), (0, [2, 4]), (1, [3, 5]), (0, [6, 8]), (1, [7, 9])],
        ),
        # by with by exception, yield all pending groups FIFO
        (
            stream("01-23456789"),
            2,
            lambda n: int(n) % 2,
            None,
            [
                (0, ["0"]),
                (1, ["1"]),
                (0, ["2", "4"]),
                (1, ["3", "5"]),
                (0, ["6", "8"]),
                (1, ["7", "9"]),
            ],
        ),
        # within
        (
            stream(range(10)),
            None,
            None,
            timedelta(seconds=3.9 * SLOW_IDENTITY_DURATION),
            [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]],
        ),
        # within + by + up_to: within triggering before up_to triggers
        (
            stream(range(10)),
            3,
            lambda n: n % 2,
            timedelta(seconds=3.9 * SLOW_IDENTITY_DURATION),
            [
                (0, [0, 2]),
                (1, [1, 3]),
                (0, [4, 6]),
                (1, [5, 7]),
                (0, [8]),
                (1, [9]),
            ],
        ),
        # within + by + up_to: up_to sometimes triggering before within
        (
            stream(range(10)),
            2,
            lambda n: n == 0,
            timedelta(seconds=7.9 * SLOW_IDENTITY_DURATION),
            [
                (False, [1, 2]),  # up_to triggers
                (False, [3, 4]),  # up_to triggers
                (False, [5, 6]),  # up_to triggers
                (True, [0]),  # within triggers
                (False, [7, 8]),  # up_to triggers
                (False, [9]),  # up_to triggers
            ],
        ),
        # within + by with upstream exception, yield all pending groups FIFO
        # when within triggers it only yields the oldest group
        (
            stream("01-23456789").map(int),
            None,
            lambda n: n % 2,
            timedelta(seconds=3.9 * SLOW_IDENTITY_DURATION),
            [(0, [0]), (1, [1]), (0, [2, 4]), (1, [3, 5]), (0, [6, 8]), (1, [7, 9])],
        ),
        # within + by with by exception, yield all pending groups FIFO
        # when within triggers it only yields the oldest group
        (
            stream("01-23456789"),
            None,
            lambda n: int(n) % 2,
            timedelta(seconds=3.9 * SLOW_IDENTITY_DURATION),
            [
                (0, ["0"]),
                (1, ["1"]),
                (0, ["2", "4"]),
                (1, ["3", "5"]),
                (0, ["6", "8"]),
                (1, ["7", "9"]),
            ],
        ),
        # within faster than upstream
        (
            stream(range(10)),
            None,
            None,
            timedelta(seconds=0.9 * SLOW_IDENTITY_DURATION),
            [[elem] for elem in range(10)],
        ),
    ],
)
@pytest.mark.parametrize(
    "itype, slow_identity, adapt",
    [
        (Iterable, slow_identity, identity),
        (Iterable, async_slow_identity, asyncify),
        (AsyncIterable, async_slow_identity, asyncify),
    ],
)
def test_group_cases(
    itype: IterableType,
    upstream: stream[Any],
    up_to: int,
    by: Optional[Callable[[int], int]],
    within: Optional[datetime.timedelta],
    expected: List[List[int]],
    adapt: Callable[[Any], Any],
    slow_identity: Callable[[Any], Any],
) -> None:
    s = (
        upstream.map(slow_identity)
        .group(up_to=up_to, by=adapt(by), within=within)
        .catch(ValueError)
    )
    assert alist_or_list(s, itype) == expected


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
@pytest.mark.asyncio
async def test_group_within_does_not_pull_ahead_of_consumption(
    itype: IterableType,
) -> None:
    """
    Test that the `group` operator does not continue to pull upstream elements when the next group is not requested.
    Only the upstream element pending when `within` elapses is pulled after yield.
    """
    pulled: List[int] = []
    per = 0.1
    s = (
        ints.throttle(1, per=timedelta(seconds=per))
        .do(pulled.append)
        .group(within=timedelta(seconds=4.9 * per))
    )
    it = aiter_or_iter(s, itype)
    if isinstance(it, Iterable):
        assert it.__next__() == list(INTEGERS)[:5]
        time.sleep(2 * per)
    else:
        assert await it.__anext__() == list(INTEGERS)[:5]
        await asyncio.sleep(2 * per)
    assert pulled == list(INTEGERS)[:6]
