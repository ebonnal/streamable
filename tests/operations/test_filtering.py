import builtins
import sys

import pytest

from streamable import stream
from streamable._utils._func import asyncify
from tests.utils import (
    ITERABLE_TYPES,
    IterableType,
    N,
    TestError,
    alist_or_list,
    anext_or_next,
    bi_iterable_to_iter,
    identity,
    ints_src,
    stopiteration_type,
    throw_for_odd_func,
    to_list,
)


@pytest.mark.parametrize(
    "itype, adapt",
    ((itype, adapt) for adapt in (identity, asyncify) for itype in ITERABLE_TYPES),
)
def test_filter(itype: IterableType, adapt) -> None:
    def keep(x) -> int:
        return x % 2

    # `filter` must act like builtin filter
    assert to_list(stream(ints_src).filter(adapt(keep)), itype=itype) == list(
        builtins.filter(keep, ints_src)
    )
    # `filter` with `bool` as predicate must act like builtin filter with None predicate.
    assert to_list(stream(ints_src).filter(adapt(bool)), itype=itype) == list(
        builtins.filter(None, ints_src)
    )
    # `filter` with `bool` as predicate must act like builtin filter with None predicate.
    assert to_list(stream(ints_src).filter(), itype=itype) == list(
        builtins.filter(None, ints_src)
    )


@pytest.mark.parametrize(
    "itype, adapt",
    ((itype, adapt) for adapt in (identity, asyncify) for itype in ITERABLE_TYPES),
)
def test_skip(itype: IterableType, adapt) -> None:
    # `skip` must raise ValueError if `until` is negative
    with pytest.raises(ValueError, match="`until` must be >= 0 but got -1"):
        stream(ints_src).skip(-1)
    for count in [0, 1, 3]:
        # `skip` must skip `until` elements
        assert (
            to_list(stream(ints_src).skip(count), itype=itype) == list(ints_src)[count:]
        )
        # `skip` should not count exceptions as skipped elements
        assert (
            to_list(
                stream(map(throw_for_odd_func(TestError), ints_src))
                .skip(count)
                .catch(TestError),
                itype=itype,
            )
            == list(filter(lambda i: i % 2 == 0, ints_src))[count:]
        )
        # `skip` must yield starting from the first element satisfying `until`
        assert (
            to_list(stream(ints_src).skip(adapt(lambda n: n >= count)), itype=itype)
            == list(ints_src)[count:]
        )


@pytest.mark.parametrize(
    "itype, adapt",
    ((itype, adapt) for adapt in (identity, asyncify) for itype in ITERABLE_TYPES),
)
def test_take(itype: IterableType, adapt) -> None:
    # `take` must be ok with `until` >= stream length
    assert to_list(stream(ints_src).take(N * 2), itype=itype) == list(ints_src)
    # `take` must be ok with `until` >= 1
    assert to_list(stream(ints_src).take(2), itype=itype) == [0, 1]
    # `take` must be ok with `until` == 1
    assert to_list(stream(ints_src).take(1), itype=itype) == [0]
    # `take` must be ok with `until` == 0
    assert to_list(stream(ints_src).take(0), itype=itype) == []
    # `take` must raise ValueError if `until` is negative
    with pytest.raises(
        ValueError,
        match="`until` must be >= 0 but got -1",
    ):
        stream(ints_src).take(-1)

    # `take` must be no-op if `until` greater than source's size
    assert to_list(stream(ints_src).take(sys.maxsize), itype=itype) == list(ints_src)
    count = N // 2
    raising_stream_iterator = bi_iterable_to_iter(
        stream(map(lambda x: round((1 / x) * x**2), ints_src)).take(count),
        itype=itype,
    )
    # `take` should not stop iteration when encountering exceptions and raise them without counting them...
    with pytest.raises(ZeroDivisionError):
        anext_or_next(raising_stream_iterator)
    assert alist_or_list(raising_stream_iterator) == list(range(1, count + 1))
    # ... and after reaching the limit it still continues to raise StopIteration on calls to next
    with pytest.raises(stopiteration_type(type(raising_stream_iterator))):
        anext_or_next(raising_stream_iterator)

    iter_take_on_predicate = bi_iterable_to_iter(
        stream(ints_src).take(until=adapt(lambda n: n == 5)), itype=itype
    )
    # `until` n == 5 must be equivalent to `until` = 5
    assert alist_or_list(iter_take_on_predicate) == to_list(
        stream(ints_src).take(5), itype=itype
    )
    # After exhaustion a call to __next__ on a take iterator must raise StopIteration
    with pytest.raises(stopiteration_type(type(iter_take_on_predicate))):
        anext_or_next(iter_take_on_predicate)
    # an exception raised by `until` must be raised
    with pytest.raises(ZeroDivisionError):
        to_list(stream(ints_src).take(until=adapt(lambda _: 1 / 0)), itype=itype)
