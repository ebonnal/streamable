import sys
from typing import Any, Callable, List

import pytest

from streamable import stream
from streamable._tools._func import asyncify
from tests.utils.func import identity
from tests.utils.iter import ITERABLE_TYPES, IterableType, alist_or_list
from tests.utils.source import INTEGERS, ints


def test_skip_raises_on_negative_until() -> None:
    with pytest.raises(ValueError, match="`until` must be >= 0 but got -1"):
        ints.skip(-1)


@pytest.mark.parametrize(
    "upstream, until, expected",
    [
        # empty upstream
        (stream([]), 0, []),
        # skip nothing
        (ints, 0, list(INTEGERS)),
        # skip 1 element
        (ints, 1, list(INTEGERS)[1:]),
        # skip 2 elements
        (ints, 2, list(INTEGERS)[2:]),
        # skip all elements if limit equals stream length
        (ints, len(INTEGERS), []),
        # skip all elements if limit exceeds stream length
        (ints, sys.maxsize, []),
        # predicate
        (stream("1234-56"), lambda c: c == "-", ["-", "5", "6"]),
        # does not count upstream exceptions
        (stream("1-2-3-4").map(int), 3, [4]),
        # does not count predicate exceptions
        (stream("1-2-3-4"), lambda c: int(c) == 2, ["2", "-", "3", "-", "4"]),
    ],
)
@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_skip_cases(
    itype: IterableType,
    upstream: stream[int],
    until: int,
    expected: List[int],
    adapt: Callable[[Any], Any],
) -> None:
    s = upstream.skip(until=adapt(until) if callable(until) else until).catch(
        ValueError
    )
    assert alist_or_list(s, itype) == expected
