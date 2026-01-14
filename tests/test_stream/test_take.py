import sys
from typing import Any, Callable, List

import pytest

from streamable import stream
from streamable._tools._func import asyncify
from tests.utils.func import identity
from tests.utils.iter import (
    ITERABLE_TYPES,
    IterableType,
    aiter_or_iter,
    alist_or_list,
    anext_or_next,
    stopiteration_type,
)
from tests.utils.source import INTEGERS, ints


def test_take_raises_on_negative_limit() -> None:
    with pytest.raises(
        ValueError,
        match="`until` must be >= 0 but got -1",
    ):
        ints.take(-1)


@pytest.mark.parametrize(
    "upstream, until, expected",
    [
        # empty upstream
        (stream([]), 1, []),
        # take nothing
        (ints, 0, []),
        # limit is 1
        (ints, 1, [0]),
        # limit is 2
        (ints, 2, [0, 1]),
        # yield all elements if limit equals stream length
        (ints, len(INTEGERS), list(INTEGERS)),
        # yield all elements if limit exceeds stream length
        (ints, sys.maxsize, list(INTEGERS)),
        # predicate
        (stream("1234-56"), lambda c: c == "-", ["1", "2", "3", "4"]),
        # does not count upstream exceptions
        (stream("1-2-3-4").map(int), 3, [1, 2, 3]),
        # does not count predicate exceptions
        (stream("1-2-3-4"), lambda c: int(c) == 4, ["1", "2", "3"]),
    ],
)
@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_take_cases(
    itype: IterableType,
    upstream: stream[int],
    until: int,
    expected: List[int],
    adapt: Callable[[Any], Any],
) -> None:
    s = upstream.take(until=adapt(until) if callable(until) else until).catch(
        ValueError
    )
    assert alist_or_list(s, itype) == expected


@pytest.mark.parametrize(
    "taking_stream",
    [
        stream(range(10)).take(until=lambda n: n == 2),
        stream(range(10)).take(until=asyncify(lambda n: n == 2)),
        stream(range(10)).take(until=2),
    ],
)
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_take_additional_nexts(itype: IterableType, taking_stream: stream[int]) -> None:
    """
    Take should raise StopIteration after the limit is reached once,
    even if the predicate is not satisfied anymore.
    """
    it = aiter_or_iter(taking_stream, itype)
    assert anext_or_next(it, itype) == 0
    assert anext_or_next(it, itype) == 1
    with pytest.raises(stopiteration_type(itype)):
        anext_or_next(it, itype)
    with pytest.raises(stopiteration_type(itype)):
        anext_or_next(it, itype)
