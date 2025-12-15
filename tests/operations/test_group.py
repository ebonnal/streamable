import datetime
from operator import itemgetter
from typing import (
    AsyncIterator,
    Iterator,
    List,
    Tuple,
    Union,
)

import pytest

from streamable import stream
from streamable._utils._func import asyncify
from tests.utils import (
    ITERABLE_TYPES,
    IterableType,
    N,
    TestError,
    anext_or_next,
    bi_iterable_to_iter,
    even_src,
    identity,
    slow_identity,
    slow_identity_duration,
    ints_src,
    src_raising_at_exhaustion,
    to_list,
)


@pytest.mark.parametrize(
    "itype, adapt",
    ((itype, adapt) for adapt in (identity, asyncify) for itype in ITERABLE_TYPES),
)
def test_group(itype: IterableType, adapt) -> None:
    # `group` should raise error when called with `seconds` <= 0.
    for seconds in [-1, 0]:
        with pytest.raises(
            ValueError,
            match="`every` must be a positive timedelta but got datetime\.timedelta(.*)",
        ):
            to_list(
                stream([1]).group(up_to=100, every=datetime.timedelta(seconds=seconds)),
                itype=itype,
            )

    # `group` should raise error when called with `up_to` < 1.
    for size in [-1, 0]:
        with pytest.raises(ValueError):
            to_list(stream([1]).group(up_to=size), itype=itype)

    # group size
    assert to_list(stream(range(6)).group(up_to=4), itype=itype) == [
        [0, 1, 2, 3],
        [4, 5],
    ]
    assert to_list(stream(range(6)).group(up_to=2), itype=itype) == [
        [0, 1],
        [2, 3],
        [4, 5],
    ]
    assert to_list(stream([]).group(up_to=2), itype=itype) == []

    # behavior with exceptions
    def f(i):
        return i / (110 - i)

    stream_iterator = bi_iterable_to_iter(
        stream(map(f, ints_src)).group(100), itype=itype
    )
    anext_or_next(stream_iterator)
    # when encountering upstream exception, `group` should yield the current accumulated group...
    assert anext_or_next(stream_iterator) == list(map(f, range(100, 110)))
    # ... and raise the upstream exception during the next call to `next`...
    with pytest.raises(ZeroDivisionError):
        anext_or_next(stream_iterator)

    # ... and restarting a fresh group to yield after that.
    assert anext_or_next(stream_iterator) == list(map(f, range(111, 211)))

    # behavior of the `every` parameter
    # `group` should not yield empty groups even though `every` if smaller than upstream's frequency
    assert to_list(
        stream(map(slow_identity, ints_src)).group(
            up_to=100,
            every=datetime.timedelta(seconds=slow_identity_duration / 1000),
        ),
        itype=itype,
    ) == list(map(lambda e: [e], ints_src))
    # `group` with `by` argument should not yield empty groups even though `every` if smaller than upstream's frequency
    assert to_list(
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
    assert to_list(
        stream(map(slow_identity, ints_src)).group(
            up_to=100,
            every=datetime.timedelta(seconds=2 * slow_identity_duration * 0.99),
        ),
        itype=itype,
    ) == list(map(lambda e: [e, e + 1], even_src))

    # `group` without arguments should group the elements all together
    assert anext_or_next(
        bi_iterable_to_iter(stream(ints_src).group(), itype=itype)
    ) == list(ints_src)

    groupby_stream_iter: Union[
        Iterator[Tuple[int, List[int]]], AsyncIterator[Tuple[int, List[int]]]
    ] = bi_iterable_to_iter(
        stream(ints_src).group(by=adapt(lambda n: n % 2), up_to=2), itype=itype
    )
    # `group` `by` must cogroup elements.
    assert [
        anext_or_next(groupby_stream_iter),
        anext_or_next(groupby_stream_iter),
    ] == [(0, [0, 2]), (1, [1, 3])]

    # test by
    stream_iter = bi_iterable_to_iter(
        stream(ints_src).group(up_to=2, by=adapt(lambda n: n % 2)).map(itemgetter(1)),
        itype=itype,
    )
    # `group` called with a `by` function must cogroup elements.
    assert [anext_or_next(stream_iter), anext_or_next(stream_iter)] == [
        [0, 2],
        [1, 3],
    ]
    # `group` called with a `by` function and a `up_to` should yield the first batch becoming full.
    assert anext_or_next(
        bi_iterable_to_iter(
            stream(src_raising_at_exhaustion())
            .group(
                up_to=10,
                by=adapt(lambda n: n % 4 != 0),
            )
            .map(itemgetter(1)),
            itype=itype,
        ),
    ) == [1, 2, 3, 5, 6, 7, 9, 10, 11, 13]
    # `group` called with a `by` function and an infinite size must cogroup elements and yield groups starting with the group containing the oldest element.
    assert to_list(
        stream(ints_src).group(by=adapt(lambda n: n % 2)).map(itemgetter(1)),
        itype=itype,
    ) == [
        list(range(0, N, 2)),
        list(range(1, N, 2)),
    ]
    # `group` called with a `by` function and reaching exhaustion must cogroup elements and yield uncomplete groups starting with the group containing the oldest element, even though it's not the largest.
    assert to_list(
        stream(range(10)).group(by=adapt(lambda n: n % 4 == 0)).map(itemgetter(1)),
        itype=itype,
    ) == [[0, 4, 8], [1, 2, 3, 5, 6, 7, 9]]

    stream_iter = bi_iterable_to_iter(
        stream(src_raising_at_exhaustion())
        .group(by=adapt(lambda n: n % 2))
        .map(itemgetter(1)),
        itype=itype,
    )
    # `group` called with a `by` function and encountering an exception must cogroup elements and yield uncomplete groups starting with the group containing the oldest element.
    assert [anext_or_next(stream_iter), anext_or_next(stream_iter)] == [
        list(range(0, N, 2)),
        list(range(1, N, 2)),
    ]
    # `group` called with a `by` function and encountering an exception must raise it after all groups have been yielded
    with pytest.raises(TestError):
        anext_or_next(stream_iter)

    # test `group` `by` FIFO yield on exhaustion
    assert to_list(
        stream([1, 2, 3, 3, 2, 1]).group(by=adapt(identity)), itype=itype
    ) == [(1, [1, 1]), (2, [2, 2]), (3, [3, 3])]

    # test `group` `by` FIFO yield on upstream exception
    assert to_list(
        stream([1, 2, 2, 0, 3, 1, 3, 2, 2, 3])
        .do(adapt(lambda n: 1 / n))
        .group(by=adapt(identity))
        .catch(ZeroDivisionError),
        itype=itype,
    ) == [(1, [1]), (2, [2, 2]), (3, [3, 3, 3]), (1, [1]), (2, [2, 2])]

    # test `group` `by` FIFO yield on `by` exception
    assert to_list(
        stream([1, 2, 2, 0, 3, 1, 3, 2, 2, 3])
        .group(by=adapt(lambda n: 1 / n))
        .catch(ZeroDivisionError),
        itype=itype,
    ) == [(1, [1]), (1 / 2, [2, 2]), (1 / 3, [3, 3, 3]), (1, [1]), (1 / 2, [2, 2])]

    # test `group` `by` FIFO yield on `every` elapsed
    assert to_list(
        stream(map(slow_identity, [1, 2, 2, 2, 2, 3, 3, 1, 3]))
        .group(
            by=adapt(identity),
            every=datetime.timedelta(seconds=2.9 * slow_identity_duration),
        )
        .catch(ZeroDivisionError),
        itype=itype,
    ) == [(1, [1]), (2, [2, 2, 2, 2]), (3, [3, 3, 3]), (1, [1])]
