import datetime
from operator import itemgetter
from typing import Any, AsyncIterator, Callable, Iterator, List, Tuple, Union

import pytest

from streamable import stream
from streamable._tools._func import asyncify
from tests.utils.error import TestError
from tests.utils.functions import (
    identity,
    slow_identity,
    slow_identity_duration,
    src_raising_at_exhaustion,
)
from tests.utils.iteration import (
    ITERABLE_TYPES,
    IterableType,
    alist_or_list,
    anext_or_next,
    aiter_or_iter,
)
from tests.utils.source import N, even_src, ints_src


# ============================================================================
# Validation Tests
# ============================================================================


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_group_raises_on_invalid_every(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Group should raise error when called with `every` <= 0."""
    for seconds in [-1, 0]:
        with pytest.raises(
            ValueError,
            match="`every` must be a positive timedelta but got datetime\.timedelta(.*)",
        ):
            alist_or_list(
                stream([1]).group(up_to=100, every=datetime.timedelta(seconds=seconds)),
                itype=itype,
            )


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_group_raises_on_invalid_up_to(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Group should raise error when called with `up_to` < 1."""
    for size in [-1, 0]:
        with pytest.raises(ValueError):
            alist_or_list(stream([1]).group(up_to=size), itype=itype)


# ============================================================================
# Basic Grouping Tests
# ============================================================================


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_group_by_size(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Group should collect elements into batches of specified size."""
    assert alist_or_list(stream(range(6)).group(up_to=4), itype=itype) == [
        [0, 1, 2, 3],
        [4, 5],
    ]
    assert alist_or_list(stream(range(6)).group(up_to=2), itype=itype) == [
        [0, 1],
        [2, 3],
        [4, 5],
    ]
    assert alist_or_list(stream([]).group(up_to=2), itype=itype) == []


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_group_without_arguments(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Group without arguments should group all elements together."""
    assert anext_or_next(
        aiter_or_iter(stream(ints_src).group(), itype=itype),
        itype=itype,
    ) == list(ints_src)


# ============================================================================
# Exception Handling Tests
# ============================================================================


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_group_with_exceptions(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Group should handle exceptions correctly."""

    def f(i):
        return i / (110 - i)

    stream_iterator = aiter_or_iter(stream(map(f, ints_src)).group(100), itype=itype)
    anext_or_next(stream_iterator, itype=itype)
    # when encountering upstream exception, `group` should yield the current accumulated group...
    assert anext_or_next(stream_iterator, itype=itype) == list(map(f, range(100, 110)))
    # ... and raise the upstream exception during the next call to `next`...
    with pytest.raises(ZeroDivisionError):
        anext_or_next(stream_iterator, itype=itype)

    # ... and restarting a fresh group to yield after that.
    assert anext_or_next(stream_iterator, itype=itype) == list(map(f, range(111, 211)))


# ============================================================================
# Time-Based Grouping Tests
# ============================================================================


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_group_every_parameter(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Group should respect the `every` time interval parameter."""
    # `group` should not yield empty groups even though `every` if smaller than upstream's frequency
    assert alist_or_list(
        stream(map(slow_identity, ints_src)).group(
            up_to=100,
            every=datetime.timedelta(seconds=slow_identity_duration / 1000),
        ),
        itype=itype,
    ) == list(map(lambda e: [e], ints_src))
    # `group` with `by` argument should not yield empty groups even though `every` if smaller than upstream's frequency
    assert alist_or_list(
        stream(map(slow_identity, ints_src))
        .group(
            up_to=100,
            every=datetime.timedelta(seconds=slow_identity_duration / 1000),
            by=adapt(lambda _: None),
        )
        .map(itemgetter(1)),
        itype=itype,
    ) == list(map(lambda e: [e], ints_src))
    # `group` should yield upstream elements in a two-element group if `every` inferior to twice the upstream yield period
    assert alist_or_list(
        stream(map(slow_identity, ints_src)).group(
            up_to=100,
            every=datetime.timedelta(seconds=2 * slow_identity_duration * 0.99),
        ),
        itype=itype,
    ) == list(map(lambda e: [e, e + 1], even_src))


# ============================================================================
# Group By Key Tests
# ============================================================================


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_group_by_key_basic(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Group by key function should cogroup elements."""
    groupby_stream_iter: Union[
        Iterator[Tuple[int, List[int]]], AsyncIterator[Tuple[int, List[int]]]
    ] = aiter_or_iter(
        stream(ints_src).group(by=adapt(lambda n: n % 2), up_to=2), itype=itype
    )
    # `group` `by` must cogroup elements.
    assert [
        anext_or_next(groupby_stream_iter, itype=itype),
        anext_or_next(groupby_stream_iter, itype=itype),
    ] == [(0, [0, 2]), (1, [1, 3])]


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_group_by_key_with_up_to(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Group by key with up_to should yield first batch becoming full."""
    stream_iter = aiter_or_iter(
        stream(ints_src).group(up_to=2, by=adapt(lambda n: n % 2)).map(itemgetter(1)),
        itype=itype,
    )
    # `group` called with a `by` function must cogroup elements.
    assert [
        anext_or_next(stream_iter, itype=itype),
        anext_or_next(stream_iter, itype=itype),
    ] == [
        [0, 2],
        [1, 3],
    ]
    # `group` called with a `by` function and a `up_to` should yield the first batch becoming full.
    assert anext_or_next(
        aiter_or_iter(
            stream(src_raising_at_exhaustion())
            .group(
                up_to=10,
                by=adapt(lambda n: n % 4 != 0),
            )
            .map(itemgetter(1)),
            itype=itype,
        ),
        itype=itype,
    ) == [1, 2, 3, 5, 6, 7, 9, 10, 11, 13]


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_group_by_key_infinite_size(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Group by key with infinite size must cogroup elements and yield groups starting with the group containing the oldest element."""
    # `group` called with a `by` function and an infinite size must cogroup elements and yield groups starting with the group containing the oldest element.
    assert alist_or_list(
        stream(ints_src).group(by=adapt(lambda n: n % 2)).map(itemgetter(1)),
        itype=itype,
    ) == [
        list(range(0, N, 2)),
        list(range(1, N, 2)),
    ]


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_group_by_key_on_exhaustion(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Group by key on exhaustion must yield incomplete groups starting with the oldest element."""
    # `group` called with a `by` function and reaching exhaustion must cogroup elements and yield uncomplete groups starting with the group containing the oldest element, even though it's not the largest.
    assert alist_or_list(
        stream(range(10)).group(by=adapt(lambda n: n % 4 == 0)).map(itemgetter(1)),
        itype=itype,
    ) == [[0, 4, 8], [1, 2, 3, 5, 6, 7, 9]]


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_group_by_key_with_exceptions(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Group by key with exceptions must cogroup and yield incomplete groups, then raise."""
    stream_iter = aiter_or_iter(
        stream(src_raising_at_exhaustion())
        .group(by=adapt(lambda n: n % 2))
        .map(itemgetter(1)),
        itype=itype,
    )
    # `group` called with a `by` function and encountering an exception must cogroup elements and yield uncomplete groups starting with the group containing the oldest element.
    assert [
        anext_or_next(stream_iter, itype=itype),
        anext_or_next(stream_iter, itype=itype),
    ] == [
        list(range(0, N, 2)),
        list(range(1, N, 2)),
    ]
    # `group` called with a `by` function and encountering an exception must raise it after all groups have been yielded
    with pytest.raises(TestError):
        anext_or_next(stream_iter, itype=itype)


# ============================================================================
# FIFO Yield Tests
# ============================================================================


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_group_by_fifo_yield_on_exhaustion(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Group by key should yield groups in FIFO order on exhaustion."""
    # test `group` `by` FIFO yield on exhaustion
    assert alist_or_list(
        stream([1, 2, 3, 3, 2, 1]).group(by=adapt(str)), itype=itype
    ) == [
        ("1", [1, 1]),
        ("2", [2, 2]),
        ("3", [3, 3]),
    ]


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_group_by_fifo_yield_on_upstream_exception(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Group by key should yield groups in FIFO order on upstream exception."""
    # test `group` `by` FIFO yield on upstream exception
    assert alist_or_list(
        stream([1, 2, 2, 0, 3, 1, 3, 2, 2, 3])
        .do(adapt(lambda n: 1 / n))
        .group(by=adapt(str))
        .catch(ZeroDivisionError),
        itype=itype,
    ) == [("1", [1]), ("2", [2, 2]), ("3", [3, 3, 3]), ("1", [1]), ("2", [2, 2])]


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_group_by_fifo_yield_on_by_exception(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Group by key should yield groups in FIFO order when by function raises."""
    # test `group` `by` FIFO yield on `by` exception
    assert alist_or_list(
        stream([1, 2, 2, 0, 3, 1, 3, 2, 2, 3])
        .group(by=adapt(lambda n: 1 / n))
        .catch(ZeroDivisionError),
        itype=itype,
    ) == [(1, [1]), (1 / 2, [2, 2]), (1 / 3, [3, 3, 3]), (1, [1]), (1 / 2, [2, 2])]


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_group_by_fifo_yield_on_every_elapsed(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Group by key should yield groups in FIFO order when every interval elapses."""
    # test `group` `by` FIFO yield on `every` elapsed
    assert alist_or_list(
        stream(map(slow_identity, [1, 2, 2, 2, 2, 3, 3, 1, 3]))
        .group(
            by=adapt(str),
            every=datetime.timedelta(seconds=2.9 * slow_identity_duration),
        )
        .catch(ZeroDivisionError),
        itype=itype,
    ) == [("1", [1]), ("2", [2, 2, 2, 2]), ("3", [3, 3, 3]), ("1", [1])]
