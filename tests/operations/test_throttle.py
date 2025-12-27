import datetime
import math
import time

import pytest

from streamable import stream
from tests.utils.error import TestError
from tests.utils.functions import throw_func
from tests.utils.iteration import (
    ITERABLE_TYPES,
    IterableType,
    alist_or_list,
    anext_or_next,
    aiter_or_iter,
)
from tests.utils.source import ints_src
from tests.utils.timing import timestream


# ============================================================================
# Validation Tests
# ============================================================================


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_throttle_raises_on_zero_timedelta(itype: IterableType) -> None:
    """Throttle should raise ValueError when per is zero or negative."""
    with pytest.raises(
        ValueError,
        match=r"`per` must be a positive timedelta but got datetime\.timedelta\(0\)",
    ):
        alist_or_list(
            stream([1]).throttle(1, per=datetime.timedelta(microseconds=0)),
            itype=itype,
        )


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_throttle_raises_on_invalid_count(itype: IterableType) -> None:
    """Throttle should raise ValueError when count is less than 1."""
    with pytest.raises(ValueError, match="`up_to` must be >= 1 but got 0"):
        alist_or_list(
            stream([1]).throttle(0, per=datetime.timedelta(seconds=1)), itype=itype
        )


# ============================================================================
# Interval Timing Tests
# ============================================================================


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_throttle_enforces_minimum_interval(itype: IterableType) -> None:
    """
    Throttle should enforce minimum interval between emissions.

    When throttling to 1 per 0.3s, 10 items should take approximately
    2.7 seconds (9 intervals * 0.3s) plus any upstream delays.
    """
    interval_seconds = 0.3
    super_slow_elem_pull_seconds = 2 * interval_seconds
    N = 10
    integers = range(N)

    def slow_first_elem(elem: int):
        if elem == 0:
            time.sleep(super_slow_elem_pull_seconds)
        return elem

    stream_ = stream(map(slow_first_elem, integers)).throttle(
        1, per=datetime.timedelta(seconds=interval_seconds)
    )
    duration, res = timestream(stream_, itype=itype)
    # `throttle` with `interval` must yield upstream elements
    assert res == list(integers)
    expected_duration = (N - 1) * interval_seconds + super_slow_elem_pull_seconds
    # avoid bursts after very slow particular upstream elements
    assert duration == pytest.approx(expected_duration, rel=0.1)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_throttle_with_exceptions_respects_interval(itype: IterableType) -> None:
    """Throttle should enforce interval even when upstream raises exceptions."""
    interval_seconds = 0.3
    super_slow_elem_pull_seconds = 2 * interval_seconds
    N = 10
    integers = range(N)

    def slow_first_elem(elem: int):
        if elem == 0:
            time.sleep(super_slow_elem_pull_seconds)
        return elem

    stream_ = (
        stream(map(throw_func(TestError), map(slow_first_elem, integers)))
        .throttle(1, per=datetime.timedelta(seconds=interval_seconds))
        .catch(TestError)
    )
    duration, res = timestream(stream_, itype=itype)
    # `throttle` with `interval` must yield upstream elements
    assert res == []
    expected_duration = (N - 1) * interval_seconds + super_slow_elem_pull_seconds
    # avoid bursts after very slow particular upstream elements
    assert duration == pytest.approx(expected_duration, rel=0.1)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_throttle_handles_slow_upstream(itype: IterableType) -> None:
    """
    Throttle should not raise ValueError when upstream is slower than interval.

    This tests that sleep calculations don't go negative when upstream
    processing takes longer than the throttle interval.
    """
    # `throttle` should avoid 'ValueError: sleep length must be non-negative' when upstream is slower than `interval`
    assert (
        anext_or_next(
            aiter_or_iter(
                stream(ints_src)
                .throttle(1, per=datetime.timedelta(seconds=0.2))
                .throttle(1, per=datetime.timedelta(seconds=0.1)),
                itype=itype,
            ),
            itype=itype,
        )
        == 0
    )


# ============================================================================
# Rate Limiting Tests (per second)
# ============================================================================


@pytest.mark.parametrize("n_items", [1, 10, 11])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_throttle_limits_per_second(n_items: int, itype: IterableType) -> None:
    """
    Throttle should limit emissions per second correctly.

    With throttle(2, per=1s), N items should take approximately
    ceil(N/2) - 1 seconds.
    """
    integers = range(n_items)
    per_second = 2
    stream_ = stream(integers).throttle(per_second, per=datetime.timedelta(seconds=1))
    duration, res = timestream(stream_, itype=itype)
    # `throttle` with `per_second` must yield upstream elements
    assert res == list(integers)
    expected_duration = math.ceil(n_items / per_second) - 1
    # `throttle` must slow according to `per_second`
    assert duration == pytest.approx(
        expected_duration, abs=0.01 * expected_duration + 0.01
    )


@pytest.mark.parametrize("n_items", [1, 10, 11])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_throttle_with_exceptions_limits_per_second(
    n_items: int, itype: IterableType
) -> None:
    """Throttle should limit rate even when upstream raises exceptions."""
    integers = range(n_items)
    per_second = 2
    stream_ = (
        stream(map(throw_func(TestError), integers))
        .throttle(per_second, per=datetime.timedelta(seconds=1))
        .catch(TestError)
    )
    duration, res = timestream(stream_, itype=itype)
    # `throttle` with `per_second` must yield upstream elements
    assert res == []
    expected_duration = math.ceil(n_items / per_second) - 1
    # `throttle` must slow according to `per_second`
    assert duration == pytest.approx(
        expected_duration, abs=0.01 * expected_duration + 0.01
    )


# ============================================================================
# Chained Throttle Tests
# ============================================================================


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_throttle_chained_follows_most_restrictive(itype: IterableType) -> None:
    """
    Chained throttles should follow the most restrictive limit.

    throttle(5, per=1s) then throttle(1, per=0.01s) should result in
    approximately 2 seconds for 11 items (10 intervals * 0.2s).
    """
    expected_duration = 2
    for stream_ in [
        stream(range(11))
        .throttle(5, per=datetime.timedelta(seconds=1))
        .throttle(1, per=datetime.timedelta(seconds=0.01)),
        stream(range(11))
        .throttle(20, per=datetime.timedelta(seconds=1))
        .throttle(1, per=datetime.timedelta(seconds=0.2)),
    ]:
        duration, _ = timestream(stream_, itype=itype)
        # `throttle` with both `per_second` and `interval` set should follow the most restrictive
        assert duration == pytest.approx(expected_duration, rel=0.1)
