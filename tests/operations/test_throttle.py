import datetime
import math
import time
from typing import (
    List,
    Tuple,
    cast,
)

import pytest

from streamable import stream
from tests.utils import (
    ITERABLE_TYPES,
    IterableType,
    TestError,
    anext_or_next,
    bi_iterable_to_iter,
    ints_src,
    throw_func,
    timestream,
    to_list,
)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_throttle(itype: IterableType) -> None:
    # `throttle` should raise error when called with negative `per`.
    with pytest.raises(
        ValueError,
        match=r"`per` must be a positive timedelta but got datetime\.timedelta\(0\)",
    ):
        to_list(
            stream([1]).throttle(1, per=datetime.timedelta(microseconds=0)),
            itype=itype,
        )
        # `throttle` should raise error when called with `count` < 1.
    with pytest.raises(ValueError, match="`up_to` must be >= 1 but got 0"):
        to_list(stream([1]).throttle(0, per=datetime.timedelta(seconds=1)), itype=itype)

    # test interval
    interval_seconds = 0.3
    super_slow_elem_pull_seconds = 2 * interval_seconds
    N = 10
    integers = range(N)

    def slow_first_elem(elem: int):
        if elem == 0:
            time.sleep(super_slow_elem_pull_seconds)
        return elem

    for stream_, expected_elems in cast(
        List[Tuple[stream, List]],
        [
            (
                stream(map(slow_first_elem, integers)).throttle(
                    1, per=datetime.timedelta(seconds=interval_seconds)
                ),
                list(integers),
            ),
            (
                stream(map(throw_func(TestError), map(slow_first_elem, integers)))
                .throttle(1, per=datetime.timedelta(seconds=interval_seconds))
                .catch(TestError),
                [],
            ),
        ],
    ):
        duration, res = timestream(stream_, itype=itype)
        # `throttle` with `interval` must yield upstream elements
        assert res == expected_elems
        expected_duration = (N - 1) * interval_seconds + super_slow_elem_pull_seconds
        # avoid bursts after very slow particular upstream elements
        assert duration == pytest.approx(expected_duration, rel=0.1)
    # `throttle` should avoid 'ValueError: sleep length must be non-negative' when upstream is slower than `interval`
    assert (
        anext_or_next(
            bi_iterable_to_iter(
                stream(ints_src)
                .throttle(1, per=datetime.timedelta(seconds=0.2))
                .throttle(1, per=datetime.timedelta(seconds=0.1)),
                itype=itype,
            )
        )
        == 0
    )

    # test per_second

    for N in [1, 10, 11]:
        integers = range(N)
        per_second = 2
        for stream_, expected_elems in cast(
            List[Tuple[stream, List]],
            [
                (
                    stream(integers).throttle(
                        per_second, per=datetime.timedelta(seconds=1)
                    ),
                    list(integers),
                ),
                (
                    stream(map(throw_func(TestError), integers))
                    .throttle(per_second, per=datetime.timedelta(seconds=1))
                    .catch(TestError),
                    [],
                ),
            ],
        ):
            duration, res = timestream(stream_, itype=itype)
            # `throttle` with `per_second` must yield upstream elements
            assert res == expected_elems
            expected_duration = math.ceil(N / per_second) - 1
            # `throttle` must slow according to `per_second`
            assert duration == pytest.approx(
                expected_duration, abs=0.01 * expected_duration + 0.01
            )

    # test chain

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
