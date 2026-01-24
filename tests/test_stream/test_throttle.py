import datetime
import time
from typing import Any, Callable

import pytest

from streamable import stream
from tests.utils.error import TestError
from tests.utils.func import throw_func
from tests.utils.iter import (
    ITERABLE_TYPES,
    IterableType,
    anext_or_next,
    aiter_or_iter,
)


@pytest.mark.parametrize(
    "per,repr_pattern",
    [
        (datetime.timedelta(), r"datetime\.timedelta\(0\)"),
        (datetime.timedelta(days=-1), r"datetime\.timedelta\(days=-1\)"),
    ],
)
def test_throttle_raises_when_per_is_not_positive(
    per: datetime.timedelta, repr_pattern: str
) -> None:
    with pytest.raises(
        ValueError,
        match=rf"`per` must be a positive timedelta but got: {repr_pattern}",
    ):
        stream([1]).throttle(1, per=per)


@pytest.mark.parametrize("up_to", [0, -1])
def test_throttle_raises_when_up_to_not_positive(up_to: int) -> None:
    with pytest.raises(ValueError, match=rf"`up_to` must be >= 1 but got: {up_to}"):
        stream([1]).throttle(up_to, per=datetime.timedelta(seconds=1))


@pytest.mark.parametrize(
    "effect,result", [(str, str), (throw_func(TestError), lambda _: None)]
)
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_throttle_sliding_window(
    itype: IterableType, effect: Callable[[float], Any], result: Callable[[float], Any]
) -> None:
    """
    Throttle should forward elements and throttle the rate of emission (elements or errors).
    At any point in time, the number of elements yielded in the last `per` interval should be at most `up_to`.
    When the yield of an element would violate that constraint, the stream sleeps until the oldest element
    leaves the sliding window.
    This tests this sliding window behavior.
    """
    sleeps = [0, 0.1, 0.1, 1.7, 0.1, 0.1, 0.1, 0.1, 0.1, 3]
    bursty_stream = stream(sleeps).do(time.sleep).map(effect)

    expected_emission_timestamps = [0.0, 0.1, 0.2, 1.9, 2, 2.1, 2.2, 3.9, 4, 7]
    throttled_stream = aiter_or_iter(
        bursty_stream.throttle(4, per=datetime.timedelta(seconds=2)).catch(
            Exception, replace=lambda err: None
        ),
        itype,
    )

    start = time.perf_counter()
    for expected_emission_timestamp, elem in zip(expected_emission_timestamps, sleeps):
        # element forwarded
        assert anext_or_next(throttled_stream, itype) == result(elem)
        # throttled as expected
        assert time.perf_counter() - start == pytest.approx(
            expected_emission_timestamp, rel=0.15, abs=0.005
        )
