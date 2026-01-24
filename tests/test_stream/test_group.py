import datetime
from typing import Any, Callable, List, Optional
from datetime import timedelta
import pytest

from streamable import stream
from streamable._tools._func import asyncify
from tests.utils.func import SLOW_IDENTITY_DURATION, identity, slow_identity
from tests.utils.iter import (
    ITERABLE_TYPES,
    IterableType,
    alist_or_list,
)


@pytest.mark.parametrize(
    "every,repr_pattern",
    [
        (datetime.timedelta(), r"datetime\.timedelta\(0\)"),
        (datetime.timedelta(days=-1), r"datetime\.timedelta\(days=-1\)"),
    ],
)
def test_group_raises_on_non_positive(
    every: datetime.timedelta, repr_pattern: str
) -> None:
    with pytest.raises(
        ValueError,
        match=rf"`every` must be a positive timedelta but got: {repr_pattern}",
    ):
        stream([1]).group(up_to=100, every=every)


@pytest.mark.parametrize("up_to", [-1, 0])
def test_group_raises_on_non_positive_up_to(up_to: int) -> None:
    with pytest.raises(ValueError, match=f"`up_to` must be >= 1 but got: {up_to}"):
        stream([]).group(up_to=up_to)


@pytest.mark.parametrize(
    "upstream, up_to, by, every, expected",
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
        # every
        (
            stream(range(10)).map(slow_identity),
            None,
            None,
            timedelta(seconds=3.9 * SLOW_IDENTITY_DURATION),
            [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]],
        ),
        # every + by + up_to
        (
            stream(range(10)).map(slow_identity),
            3,
            lambda n: n % 2,
            timedelta(seconds=3.9 * SLOW_IDENTITY_DURATION),
            [
                (0, [0, 2]),  # every triggers
                (1, [1, 3, 5]),  # up_to triggers
                (0, [4, 6, 8]),  # up_to triggers
                (1, [7, 9]),  # exhausted
            ],
        ),
        # every by: yield oldest pending group when every is elapsed
        (
            stream(range(10)).map(slow_identity),
            None,
            lambda n: n % 2,
            timedelta(seconds=3.9 * SLOW_IDENTITY_DURATION),
            [(0, [0, 2]), (1, [1, 3, 5, 7]), (0, [4, 6, 8]), (1, [9])],
        ),
        # every by with upstream exception, yield all pending groups FIFO
        # when every triggers it only yields the oldest group
        (
            stream("01-23456789").map(slow_identity).map(int),
            None,
            lambda n: n % 2,
            timedelta(seconds=3.9 * SLOW_IDENTITY_DURATION),
            [(0, [0]), (1, [1]), (0, [2, 4]), (1, [3, 5, 7, 9]), (0, [6, 8])],
        ),
        # every by with by exception, yield all pending groups FIFO
        # when every triggers it only yields the oldest group
        (
            stream("01-23456789").map(slow_identity),
            None,
            lambda n: int(n) % 2,
            timedelta(seconds=3.9 * SLOW_IDENTITY_DURATION),
            [
                (0, ["0"]),
                (1, ["1"]),
                (0, ["2", "4"]),
                (1, ["3", "5", "7", "9"]),
                (0, ["6", "8"]),
            ],
        ),
    ],
)
@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_group_cases(
    itype: IterableType,
    upstream: stream[Any],
    up_to: int,
    by: Optional[Callable[[int], int]],
    every: Optional[datetime.timedelta],
    expected: List[List[int]],
    adapt: Callable[[Any], Any],
) -> None:
    s = upstream.group(up_to=up_to, by=adapt(by), every=every).catch(ValueError)
    assert alist_or_list(s, itype) == expected
