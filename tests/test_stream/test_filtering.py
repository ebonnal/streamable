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
    """Filter should behave like Python's builtin filter function."""

    def keep(x) -> int:
        return x % 2

    assert alist_or_list(stream(INTEGERS).filter(adapt(keep)), itype=itype) == list(
        builtins.filter(keep, INTEGERS)
    )
    assert alist_or_list(stream(INTEGERS).filter(adapt(bool)), itype=itype) == list(
        builtins.filter(None, INTEGERS)
    )
    assert alist_or_list(stream(INTEGERS).filter(), itype=itype) == list(
        builtins.filter(None, INTEGERS)
    )


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_skip_raises_on_negative_count(itype: IterableType) -> None:
    """Skip should raise ValueError for negative counts."""
    with pytest.raises(ValueError, match="`until` must be >= 0 but got -1"):
        stream(INTEGERS).skip(-1)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
@pytest.mark.parametrize("count", [0, 1, 3])
def test_skip_skips_elements(itype: IterableType, count: int) -> None:
    """Skip should skip the specified number of elements."""
    assert (
        alist_or_list(stream(INTEGERS).skip(count), itype=itype)
        == list(INTEGERS)[count:]
    )


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
@pytest.mark.parametrize("count", [0, 1, 3])
def test_skip_does_not_count_exceptions(itype: IterableType, count: int) -> None:
    """Skip should not count exceptions as skipped elements."""
    assert (
        alist_or_list(
            stream(map(throw_for_odd_func(TestError), INTEGERS))
            .skip(count)
            .catch(TestError),
            itype=itype,
        )
        == list(filter(lambda i: i % 2 == 0, INTEGERS))[count:]
    )


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
@pytest.mark.parametrize("count", [0, 1, 3])
def test_skip_with_predicate(
    itype: IterableType,
    adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]],
    count: int,
) -> None:
    """Skip should skip elements until predicate is satisfied."""
    assert (
        alist_or_list(
            stream(INTEGERS).skip(until=adapt(lambda n: n >= count)), itype=itype
        )
        == list(INTEGERS)[count:]
    )


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_take_limits_elements(itype: IterableType) -> None:
    """Take should limit the number of elements yielded."""
    assert alist_or_list(stream(INTEGERS).take(2), itype=itype) == [0, 1]
    assert alist_or_list(stream(INTEGERS).take(1), itype=itype) == [0]
    assert alist_or_list(stream(INTEGERS).take(0), itype=itype) == []


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_take_with_limit_exceeding_stream_length(itype: IterableType) -> None:
    """Take should return all elements when limit exceeds stream length."""
    assert alist_or_list(stream(INTEGERS).take(N * 2), itype=itype) == list(INTEGERS)
    assert alist_or_list(stream(INTEGERS).take(sys.maxsize), itype=itype) == list(
        INTEGERS
    )


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_take_raises_on_negative_limit(itype: IterableType) -> None:
    """Take should raise ValueError for negative limits."""
    with pytest.raises(
        ValueError,
        match="`until` must be >= 0 but got -1",
    ):
        stream(INTEGERS).take(-1)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_take_does_not_count_exceptions(itype: IterableType) -> None:
    """Take should not count exceptions toward the limit and should raise them."""
    count = N // 2
    raising_stream_iterator = aiter_or_iter(
        stream(map(lambda x: round((1 / x) * x**2), INTEGERS)).take(count),
        itype=itype,
    )
    with pytest.raises(ZeroDivisionError):
        anext_or_next(raising_stream_iterator, itype=itype)
    assert alist_or_list(raising_stream_iterator, itype=itype) == list(
        range(1, count + 1)
    )
    with pytest.raises(stopiteration_type(type(raising_stream_iterator))):
        anext_or_next(raising_stream_iterator, itype=itype)


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_take_with_predicate(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Take should limit elements based on a predicate function."""
    iter_take_on_predicate = aiter_or_iter(
        stream(INTEGERS).take(until=adapt(lambda n: n == 5)), itype=itype
    )
    assert alist_or_list(iter_take_on_predicate, itype=itype) == alist_or_list(
        stream(INTEGERS).take(5), itype=itype
    )
    with pytest.raises(stopiteration_type(type(iter_take_on_predicate))):
        anext_or_next(iter_take_on_predicate, itype=itype)


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_take_raises_on_predicate_exception(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Take should raise exceptions from the predicate function."""
    with pytest.raises(ZeroDivisionError):
        alist_or_list(stream(INTEGERS).take(until=adapt(lambda _: 1 / 0)), itype=itype)
