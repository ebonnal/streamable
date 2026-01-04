import builtins
import sys
from typing import Any, Callable

import pytest

from streamable import stream
from streamable._tools._func import asyncify
from tests.utils.error import TestError
from tests.utils.functions import identity, throw_for_odd_func
from tests.utils.iteration import (
    ITERABLE_TYPES,
    IterableType,
    alist_or_list,
    anext_or_next,
    aiter_or_iter,
    stopiteration_type,
)
from tests.utils.source import N, INTEGERS


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_filter(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Filter must act like builtin filter."""

    def keep(x) -> int:
        return x % 2

    # `filter` must act like builtin filter
    assert alist_or_list(stream(INTEGERS).filter(adapt(keep)), itype=itype) == list(
        builtins.filter(keep, INTEGERS)
    )
    # `filter` with `bool` as predicate must act like builtin filter with None predicate.
    assert alist_or_list(stream(INTEGERS).filter(adapt(bool)), itype=itype) == list(
        builtins.filter(None, INTEGERS)
    )
    # `filter` with `bool` as predicate must act like builtin filter with None predicate.
    assert alist_or_list(stream(INTEGERS).filter(), itype=itype) == list(
        builtins.filter(None, INTEGERS)
    )


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_skip(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Skip must skip elements correctly."""
    # `skip` must raise ValueError if `until` is negative
    with pytest.raises(ValueError, match="`until` must be >= 0 but got -1"):
        stream(INTEGERS).skip(-1)
    for count in [0, 1, 3]:
        # `skip` must skip `until` elements
        assert (
            alist_or_list(stream(INTEGERS).skip(count), itype=itype)
            == list(INTEGERS)[count:]
        )
        # `skip` should not count exceptions as skipped elements
        assert (
            alist_or_list(
                stream(map(throw_for_odd_func(TestError), INTEGERS))
                .skip(count)
                .catch(TestError),
                itype=itype,
            )
            == list(filter(lambda i: i % 2 == 0, INTEGERS))[count:]
        )
        # `skip` must yield starting from the first element satisfying `until`
        assert (
            alist_or_list(
                stream(INTEGERS).skip(until=adapt(lambda n: n >= count)), itype=itype
            )
            == list(INTEGERS)[count:]
        )


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_take(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Take must limit elements correctly."""
    # `take` must be ok with `until` >= stream length
    assert alist_or_list(stream(INTEGERS).take(N * 2), itype=itype) == list(INTEGERS)
    # `take` must be ok with `until` >= 1
    assert alist_or_list(stream(INTEGERS).take(2), itype=itype) == [0, 1]
    # `take` must be ok with `until` == 1
    assert alist_or_list(stream(INTEGERS).take(1), itype=itype) == [0]
    # `take` must be ok with `until` == 0
    assert alist_or_list(stream(INTEGERS).take(0), itype=itype) == []
    # `take` must raise ValueError if `until` is negative
    with pytest.raises(
        ValueError,
        match="`until` must be >= 0 but got -1",
    ):
        stream(INTEGERS).take(-1)

    # `take` must be no-op if `until` greater than source's size
    assert alist_or_list(stream(INTEGERS).take(sys.maxsize), itype=itype) == list(
        INTEGERS
    )
    count = N // 2
    raising_stream_iterator = aiter_or_iter(
        stream(map(lambda x: round((1 / x) * x**2), INTEGERS)).take(count),
        itype=itype,
    )
    # `take` should not stop iteration when encountering exceptions and raise them without counting them...
    with pytest.raises(ZeroDivisionError):
        anext_or_next(raising_stream_iterator, itype=itype)
    assert alist_or_list(raising_stream_iterator, itype=itype) == list(
        range(1, count + 1)
    )
    # ... and after reaching the limit it still continues to raise StopIteration on calls to next
    with pytest.raises(stopiteration_type(type(raising_stream_iterator))):
        anext_or_next(raising_stream_iterator, itype=itype)

    iter_take_on_predicate = aiter_or_iter(
        stream(INTEGERS).take(until=adapt(lambda n: n == 5)), itype=itype
    )
    # `until` n == 5 must be equivalent to `until` = 5
    assert alist_or_list(iter_take_on_predicate, itype=itype) == alist_or_list(
        stream(INTEGERS).take(5), itype=itype
    )
    # After exhaustion a call to __next__ on a take iterator must raise StopIteration
    with pytest.raises(stopiteration_type(type(iter_take_on_predicate))):
        anext_or_next(iter_take_on_predicate, itype=itype)
    # an exception raised by `until` must be raised
    with pytest.raises(ZeroDivisionError):
        alist_or_list(stream(INTEGERS).take(until=adapt(lambda _: 1 / 0)), itype=itype)
