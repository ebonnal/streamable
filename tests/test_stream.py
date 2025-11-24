import asyncio
import builtins
import copy
import datetime
import math
import queue
import sys
import threading
import time
import traceback
from collections import Counter
from functools import partial
from pickle import PickleError
from types import TracebackType
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)
from unittest.mock import patch

import pytest

from streamable import Stream
from streamable._utils._async import awaitable_to_coroutine
from streamable._utils._func import anostop, asyncify, nostop, star
from streamable._utils._iter import (
    sync_to_async_iter,
    sync_to_bi_iterable,
)
from tests.utils import (
    ITERABLE_TYPES,
    IterableType,
    N,
    TestError,
    alist_or_list,
    anext_or_next,
    async_identity,
    async_identity_sleep,
    async_randomly_slowed,
    async_slow_identity,
    async_square,
    async_throw_for_odd_func,
    async_throw_func,
    bi_iterable_to_iter,
    even_src,
    identity,
    identity_sleep,
    randomly_slowed,
    slow_identity,
    slow_identity_duration,
    square,
    src,
    src_raising_at_exhaustion,
    stopiteration_for_iter_type,
    throw,
    throw_for_odd_func,
    throw_func,
    timecoro,
    timestream,
    to_list,
)


def test_init() -> None:
    stream = Stream(src)
    # The stream's `source` must be the source argument.
    assert stream._source is src
    # "The `upstream` attribute of a base Stream's instance must be None."
    assert stream.upstream is None
    # `source` must be propagated by operations
    assert (
        Stream(src)
        .group(100)
        .flatten()
        .map(identity)
        .amap(async_identity)
        .foreach(identity)
        .aforeach(async_identity)
        .catch(Exception)
        .observe()
        .throttle(1, per=datetime.timedelta(seconds=1))
        .source
    ) is src
    # attribute `source` must be read-only
    with pytest.raises(AttributeError):
        Stream(src).source = src  # type: ignore
    # attribute `upstream` must be read-only
    with pytest.raises(AttributeError):
        Stream(src).upstream = Stream(src)  # type: ignore


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_async_src(itype) -> None:
    # a stream with an async source must be collectable as an Iterable or as AsyncIterable
    assert to_list(Stream(sync_to_async_iter(iter(src))), itype) == list(src)
    # a stream with an async source must be collectable as an Iterable or as AsyncIterable
    assert to_list(Stream(sync_to_async_iter(iter(src)).__aiter__), itype) == list(src)


def test_repr(complex_stream: Stream, complex_stream_str: str) -> None:
    assert (
        repr(Stream([]).map(star(print)))
        == "Stream([]).map(star(<built-in function print>), concurrency=1, ordered=True)"
    )
    # `repr` should work as expected on a stream with many operation
    assert str(complex_stream) == complex_stream_str
    # explanation of different streams must be different
    assert str(complex_stream) != str(complex_stream.map(str))
    # `repr` should work as expected on a stream without operation
    assert str(Stream(src)) == "Stream(range(0, 256))"
    # `repr` should return a one-liner for a stream with 1 operations
    assert str(Stream(src).skip(10)) == "Stream(range(0, 256)).skip(until=10)"
    # `repr` should return a one-liner for a stream with 2 operations
    assert (
        str(Stream(src).skip(10).skip(10))
        == "Stream(range(0, 256)).skip(until=10).skip(until=10)"
    )
    # `repr` should go to line if it exceeds than 80 chars
    assert (
        str(Stream(src).skip(10).skip(10).skip(10).skip(10))
        == """(
    Stream(range(0, 256))
    .skip(until=10)
    .skip(until=10)
    .skip(until=10)
    .skip(until=10)
)"""
    )


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_iter(itype: IterableType) -> None:
    # iter(stream) must return an Iterator.
    assert isinstance(bi_iterable_to_iter(Stream(src), itype=itype), itype)
    # Getting an Iterator from a Stream with a source not being a Union[Callable[[], Iterator], ITerable] must raise TypeError.
    with pytest.raises(
        TypeError,
        match=r"`source` must be an Iterable/AsyncIterable or a Callable\[\[\], Iterable/AsyncIterable\] but got a <class 'int'>",
    ):
        bi_iterable_to_iter(Stream(1), itype=itype)  # type: ignore
    # Getting an Iterator from a Stream with a source not being a Union[Callable[[], Iterator], ITerable] must raise TypeError.
    with pytest.raises(
        TypeError,
        match=r"`source` must be an Iterable/AsyncIterable or a Callable\[\[\], Iterable/AsyncIterable\] but got a Callable\[\[\], <class 'int'>\]",
    ):
        bi_iterable_to_iter(Stream(lambda: 1), itype=itype)  # type: ignore


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_add(itype: IterableType) -> None:
    from streamable._stream import FlattenStream

    stream = Stream(src)
    # stream addition must return a FlattenStream.
    assert isinstance(stream + stream, FlattenStream)

    stream_a = Stream(range(10))
    stream_b = Stream(range(10, 20))
    stream_c = Stream(range(20, 30))
    # `chain` must yield the elements of the first stream the move on with the elements of the next ones and so on.
    assert to_list(stream_a + stream_b + stream_c, itype=itype) == list(range(30))


@pytest.mark.parametrize(
    "concurrency, itype",
    [(concurrency, itype) for concurrency in (1, 2) for itype in ITERABLE_TYPES],
)
def test_map(concurrency, itype) -> None:
    # At any concurrency the `map` method should act as the builtin map function, transforming elements while preserving input elements order.
    assert to_list(
        Stream(src).map(randomly_slowed(square), concurrency=concurrency),
        itype=itype,
    ) == list(map(square, src))


@pytest.mark.parametrize(
    "ordered, order_mutation, itype",
    [
        (ordered, order_mutation, itype)
        for itype in ITERABLE_TYPES
        for ordered, order_mutation in [
            (True, identity),
            (False, sorted),
        ]
    ],
)
def test_process_concurrency(ordered, order_mutation, itype) -> None:
    def local_identity(x):
        return x  # pragma: no cover

    sleeps = [0.01, 1, 0.01]
    state: List[str] = []
    expected_result_list: List[str] = list(order_mutation(map(str, sleeps)))
    stream = (
        Stream(sleeps)
        .foreach(identity_sleep, concurrency=2, ordered=ordered, via="process")
        .map(str, concurrency=2, ordered=True, via="process")
        .foreach(state.append, concurrency=2, ordered=True, via="process")
        .foreach(lambda _: state.append(""), concurrency=1, ordered=True)
    )
    # process-based concurrency must correctly transform elements, respecting `ordered`...
    assert to_list(stream, itype=itype) == expected_result_list
    # ... and should not mutate main thread-bound structures.
    assert state == [""] * len(sleeps)

    if sys.version_info >= (3, 9):
        for f in [lambda x: x, local_identity]:
            # process-based concurrency should not be able to serialize a lambda or a local func
            with pytest.raises(
                (AttributeError, PickleError),
                match="<locals>",
            ):
                to_list(Stream(src).map(f, concurrency=2, via="process"), itype=itype)
        # partial iteration
        assert (
            anext_or_next(bi_iterable_to_iter(stream, itype=itype))
            == expected_result_list[0]
        )


@pytest.mark.parametrize(
    "concurrency, n_elems, itype",
    [
        (concurrency, n_elems, itype)
        for concurrency, n_elems in [
            [16, 0],
            [1, 0],
            [16, 1],
            [16, 15],
            [16, 16],
        ]
        for itype in ITERABLE_TYPES
    ],
)
def test_map_with_more_concurrency_than_elements(concurrency, n_elems, itype) -> None:
    # `map` method should act correctly when concurrency > number of elements.
    assert to_list(
        Stream(range(n_elems)).map(str, concurrency=concurrency), itype=itype
    ) == list(map(str, range(n_elems)))


@pytest.mark.parametrize(
    "itype, concurrency, ordered, expected",
    [
        (itype, concurrency, ordered, expected)
        for itype in ITERABLE_TYPES
        for concurrency in (1, 2)
        for ordered, expected in (
            (True, [float("inf"), 1.0, float("inf"), 0.5, float("inf")]),
            (False, [float("inf"), float("inf"), float("inf"), 0.5, 1.0]),
        )
        if concurrency > 1 or ordered
    ],
)
def test_catched_error_upstream_of_map(itype, concurrency, ordered, expected) -> None:
    # at any concurrency, map/foreach should not stop iteration when upstream raises
    assert (
        to_list(
            Stream([0, 1, 0, 2, 0])
            .map(lambda n: 1 / n)
            .map(identity_sleep, concurrency=concurrency, ordered=ordered)
            .catch(ZeroDivisionError, replace=lambda e: float("inf")),
            itype=itype,
        )
        == expected
    )
    # at any concurrency, amap/aforeach should not stop iteration when upstream raises
    assert (
        to_list(
            Stream([0, 1, 0, 2, 0])
            .map(lambda n: 1 / n)
            .amap(async_identity_sleep, concurrency=concurrency, ordered=ordered)
            .catch(ZeroDivisionError, replace=lambda e: float("inf")),
            itype=itype,
        )
        == expected
    )


@pytest.mark.parametrize(
    "ordered, order_mutation, expected_duration, operation, func, itype",
    [
        [ordered, order_mutation, expected_duration, operation, func, itype]
        for ordered, order_mutation, expected_duration in [
            (True, identity, 0.7),
            (False, sorted, 0.41),
        ]
        for operation, func in [
            (Stream.foreach, time.sleep),
            (Stream.map, identity_sleep),
            (Stream.aforeach, asyncio.sleep),
            (Stream.amap, async_identity_sleep),
        ]
        for itype in ITERABLE_TYPES
    ],
)
def test_mapping_ordering(
    ordered: bool,
    order_mutation: Callable[[Iterable[float]], Iterable[float]],
    expected_duration: float,
    operation,
    func,
    itype,
) -> None:
    seconds = [0.3, 0.01, 0.01, 0.4]
    duration, res = timestream(
        operation(Stream(seconds), func, ordered=ordered, concurrency=2),
        5,
        itype=itype,
    )
    # operation must respect `ordered` constraint
    assert res == list(order_mutation(seconds))
    # should reflect that unordering improves runtime by avoiding bottlenecks
    assert duration == pytest.approx(expected_duration, rel=0.2)


@pytest.mark.parametrize(
    "concurrency, itype",
    [(concurrency, itype) for concurrency in (1, 2) for itype in ITERABLE_TYPES],
)
def test_foreach(concurrency, itype) -> None:
    side_collection: Set[int] = set()

    def side_effect(x: int, func: Callable[[int], int]):
        nonlocal side_collection
        side_collection.add(func(x))

    res = to_list(
        Stream(src).foreach(
            lambda i: randomly_slowed(side_effect(i, square)),
            concurrency=concurrency,
        ),
        itype=itype,
    )
    # At any concurrency the `foreach` method should return the upstream elements in order.
    assert res == list(src)
    # At any concurrency the `foreach` method should call func on upstream elements (in any order).
    assert side_collection == set(map(square, src))


@pytest.mark.parametrize(
    "raised_exc, caught_exc, concurrency, method, throw_func, throw_for_odd_func, nostop, itype",
    [
        [
            raised_exc,
            caught_exc,
            concurrency,
            method,
            throw_func_,
            throw_for_odd_func_,
            nostop_,
            itype,
        ]
        for raised_exc, caught_exc in [
            (TestError, TestError),
            (StopIteration, RuntimeError),
        ]
        for concurrency in [1, 2]
        for method, throw_func_, throw_for_odd_func_, nostop_ in [
            (Stream.foreach, throw_func, throw_for_odd_func, nostop),
            (Stream.aforeach, async_throw_func, async_throw_for_odd_func, anostop),
            (Stream.map, throw_func, throw_for_odd_func, nostop),
            (Stream.amap, async_throw_func, async_throw_for_odd_func, anostop),
        ]
        for itype in ITERABLE_TYPES
    ],
)
def test_map_or_foreach_with_exception(
    raised_exc: Type[Exception],
    caught_exc: Type[Exception],
    concurrency: int,
    method: Callable[[Stream, Callable[[Any], int], int], Stream],
    throw_func: Callable[[Type[Exception]], Callable[[Any], int]],
    throw_for_odd_func: Callable[[Type[Exception]], Callable[[Any], int]],
    nostop: Callable[[Any], Callable[[Any], int]],
    itype: IterableType,
) -> None:
    rasing_stream: Stream[int] = method(
        Stream(iter(src)), nostop(throw_func(raised_exc)), concurrency=concurrency
    )  # type: ignore
    # At any concurrency, `map` and `foreach` and `amap` must raise.
    with pytest.raises(caught_exc):
        to_list(rasing_stream, itype=itype)
    # Only `concurrency` upstream elements should be initially pulled for processing (0 if `concurrency=1`), and 1 more should be pulled for each call to `next`.
    assert next(cast(Iterator[int], rasing_stream.source)) == (
        concurrency + 1 if concurrency > 1 else concurrency
    )
    # At any concurrency, `map` and `foreach` and `amap` should not stop after one exception occured.
    assert to_list(
        method(
            Stream(src),
            nostop(throw_for_odd_func(raised_exc)),
            concurrency=concurrency,  # type: ignore
        ).catch(caught_exc),
        itype=itype,
    ) == list(even_src)


@pytest.mark.parametrize(
    "method, func, concurrency, itype",
    [
        [method, func, concurrency, itype]
        for method, func in [
            (Stream.foreach, slow_identity),
            (Stream.aforeach, async_slow_identity),
            (Stream.map, slow_identity),
            (Stream.amap, async_slow_identity),
        ]
        for concurrency in [1, 2, 4]
        for itype in ITERABLE_TYPES
    ],
)
def test_map_or_foreach_concurrency(method, func, concurrency, itype) -> None:
    expected_iteration_duration = N * slow_identity_duration / concurrency
    duration, res = timestream(
        method(Stream(src), func, concurrency=concurrency), itype=itype
    )
    assert res == list(src)
    # Increasing the concurrency of mapping should decrease proportionnally the iteration's duration.
    assert duration == pytest.approx(expected_iteration_duration, rel=0.1)


@pytest.mark.parametrize(
    "concurrency, itype",
    [(concurrency, itype) for concurrency in (1, 100) for itype in ITERABLE_TYPES],
)
def test_amap(concurrency, itype) -> None:
    # At any concurrency the `amap` method should act as the builtin map function, transforming elements while preserving input elements order.
    assert to_list(
        Stream(src).amap(async_randomly_slowed(async_square), concurrency=concurrency),
        itype=itype,
    ) == list(map(square, src))
    stream = Stream(src).amap(identity, concurrency=concurrency)  # type: ignore
    # `amap` should raise a TypeError if a non async function is passed to it.
    with pytest.raises(
        TypeError,
        match=r"(An asyncio.Future, a coroutine or an awaitable is required)|(object int can't be used in 'await' expression)|('int' object can't be awaited)",
    ):
        anext_or_next(bi_iterable_to_iter(stream, itype=itype))


@pytest.mark.parametrize(
    "concurrency, itype",
    [(concurrency, itype) for concurrency in (1, 100) for itype in ITERABLE_TYPES],
)
def test_aforeach(concurrency, itype) -> None:
    # At any concurrency the `foreach` method must preserve input elements order.
    assert to_list(
        Stream(src).aforeach(
            async_randomly_slowed(async_square), concurrency=concurrency
        ),
        itype=itype,
    ) == list(src)
    stream = Stream(src).aforeach(identity)  # type: ignore
    # `aforeach` should raise a TypeError if a non async function is passed to it.
    with pytest.raises(
        TypeError,
        match=r"(object int can't be used in 'await' expression)|('int' object can't be awaited)",
    ):
        anext_or_next(bi_iterable_to_iter(stream, itype=itype))


def test_flatten_typing() -> None:
    flattened_iterator_stream: Stream[str] = Stream("abc").map(iter).flatten()  # noqa: F841
    flattened_list_stream: Stream[str] = Stream("abc").map(list).flatten()  # noqa: F841
    flattened_set_stream: Stream[str] = Stream("abc").map(set).flatten()  # noqa: F841
    flattened_map_stream: Stream[str] = (  # noqa: F841
        Stream("abc").map(lambda char: map(lambda x: x, char)).flatten()
    )
    flattened_filter_stream: Stream[str] = (  # noqa: F841
        Stream("abc").map(lambda char: filter(lambda _: True, char)).flatten()
    )

    flattened_asynciter_stream: Stream[str] = (  # noqa: F841
        Stream("abc").map(iter).map(sync_to_async_iter).aflatten()
    )


@pytest.mark.parametrize(
    "concurrency, itype, flatten",
    [
        (concurrency, itype, flatten)
        for concurrency in (1, 2)
        for itype in ITERABLE_TYPES
        for flatten in (Stream.flatten, Stream.aflatten)
    ],
)
def test_flatten(concurrency, itype, flatten) -> None:
    n_iterables = 32
    it = list(range(N // n_iterables))
    double_it = it + it
    iterables_stream = Stream(
        [sync_to_bi_iterable(double_it)]
        + [sync_to_bi_iterable(it) for _ in range(n_iterables)]
    )
    if concurrency == 1:
        # At concurrency == 1, `flatten` method should yield all the upstream iterables' elements in the order of a nested for loop.
        assert to_list(
            flatten(iterables_stream, concurrency=concurrency), itype=itype
        ) == [elem for iterable in iterables_stream for elem in iterable]
    else:
        # At concurrency > 1, the `flatten` method should yield all the upstream iterables' elements.
        assert Counter(
            to_list(flatten(iterables_stream, concurrency=concurrency), itype=itype)
        ) == Counter(list(it) * n_iterables + double_it)

    # At any concurrency the `flatten` method should continue flattening even if an iterable' __next__ raises an exception.
    assert to_list(
        flatten(
            Stream([[4, 3, 2, 0], [1, 0, -1], [0, -2, -3]]).map(
                lambda iterable: sync_to_bi_iterable(map(lambda n: 1 / n, iterable))
            ),
            concurrency=concurrency,
        ).catch(ZeroDivisionError, replace=lambda e: float("inf")),
        itype=itype,
    ) == (
        [
            0.25,
            1 / 3,
            0.5,
            float("inf"),
            1,
            float("inf"),
            -1,
            float("inf"),
            -0.5,
            -1 / 3,
        ]
        if concurrency == 1
        else [
            0.25,
            1,
            1 / 3,
            float("inf"),
            0.5,
            -1,
            float("inf"),
            float("inf"),
            -0.5,
            -1 / 3,
        ]
    )
    # At any concurrency the `flatten` method should continue pulling upstream iterables even if upstream raises an exception.
    assert to_list(
        flatten(
            Stream([[4, 3, 2], cast(List[int], []), [1, 0]])
            .foreach(lambda ints: 1 / len(ints))
            .map(sync_to_bi_iterable),
            concurrency=concurrency,
        ).catch(ZeroDivisionError, replace=lambda e: -1),
        itype=itype,
    ) == ([4, 3, 2, -1, 1, 0] if concurrency == 1 else [4, -1, 3, 1, 2, 0])
    # At any concurrency the `flatten` method should continue pulling upstream iterables even if upstream's __iter__ raises an exception.
    assert to_list(
        flatten(
            Stream(
                [
                    sync_to_bi_iterable([4, 3, 2]),
                    cast(List[int], None),
                    sync_to_bi_iterable([1, 0]),
                ]
            ),
            concurrency=concurrency,
        ).catch(AttributeError, replace=lambda e: -1),
        itype=itype,
    ) == ([4, 3, 2, -1, 1, 0] if concurrency == 1 else [4, -1, 3, 1, 2, 0])
    # `flatten` should not yield any element if upstream elements are empty iterables, and be resilient to recursion issue in case of successive empty upstream iterables.
    assert (
        to_list(
            flatten(
                Stream([sync_to_bi_iterable(iter([])) for _ in range(2000)]),
                concurrency=concurrency,
            ),
            itype=itype,
        )
        == []
    )
    # `flatten` should raise if an upstream element is not iterable.
    with pytest.raises((TypeError, AttributeError)):
        anext_or_next(
            bi_iterable_to_iter(
                flatten(Stream(cast(Union[Iterable, AsyncIterable], src))),
                itype=itype,
            )
        )

    # test typing with ranges
    _: Stream[int] = Stream((src, src)).flatten()


@pytest.mark.parametrize(
    "flatten, itype, slow",
    [
        (flatten, itype, slow)
        for flatten, slow in (
            (Stream.flatten, partial(Stream.map, to=slow_identity)),
            (Stream.aflatten, partial(Stream.amap, to=async_slow_identity)),
        )
        for itype in ITERABLE_TYPES
    ],
)
def test_flatten_concurrency(flatten, itype, slow) -> None:
    concurrency = 2
    iterable_size = 5
    runtime, res = timestream(
        flatten(
            Stream(
                lambda: [
                    slow(Stream(["a"] * iterable_size)),
                    slow(Stream(["b"] * iterable_size)),
                    slow(Stream(["c"] * iterable_size)),
                ]
            ),
            concurrency=concurrency,
        ),
        times=3,
        itype=itype,
    )
    # `flatten` should process 'a's and 'b's concurrently and then 'c's
    assert res == ["a", "b"] * iterable_size + ["c"] * iterable_size

    a_runtime = b_runtime = c_runtime = iterable_size * slow_identity_duration
    expected_runtime = (a_runtime + b_runtime) / concurrency + c_runtime
    # `flatten` should process 'a's and 'b's concurrently and then 'c's without concurrency
    assert runtime == pytest.approx(expected_runtime, rel=0.1)


@pytest.mark.parametrize(
    "concurrency, itype",
    [(concurrency, itype) for concurrency in [2, 4] for itype in ITERABLE_TYPES],
)
def test_partial_iteration_on_streams_using_concurrency(
    concurrency: int, itype: IterableType
) -> None:
    yielded_elems = []

    def remembering_src() -> Iterator[int]:
        nonlocal yielded_elems
        for elem in src:
            yielded_elems.append(elem)
            yield elem

    for stream, n_pulls_after_first_next in [
        (
            Stream(remembering_src).map(identity, concurrency=concurrency),
            concurrency + 1,
        ),
        (
            Stream(remembering_src).amap(async_identity, concurrency=concurrency),
            concurrency + 1,
        ),
        (
            Stream(remembering_src).foreach(identity, concurrency=concurrency),
            concurrency + 1,
        ),
        (
            Stream(remembering_src).aforeach(async_identity, concurrency=concurrency),
            concurrency + 1,
        ),
        (
            Stream(remembering_src).group(1).flatten(concurrency=concurrency),
            concurrency,
        ),
    ]:
        yielded_elems = []
        iterator = bi_iterable_to_iter(stream, itype=itype)
        time.sleep(0.5)
        # before the first call to `next` a concurrent stream should have pulled 0 upstream elements.
        assert len(yielded_elems) == 0
        anext_or_next(iterator)
        time.sleep(0.5)
        # `after the first call to `next` a concurrent stream with given concurrency should have pulled only `n_pulls_after_first_next` upstream elements.
        assert len(yielded_elems) == n_pulls_after_first_next


@pytest.mark.parametrize(
    "itype, filter, adapt",
    (
        (itype, filter, adapt)
        for filter, adapt in ((Stream.filter, identity), (Stream.afilter, asyncify))
        for itype in ITERABLE_TYPES
    ),
)
def test_filter(itype: IterableType, filter, adapt) -> None:
    def keep(x) -> int:
        return x % 2

    # `filter` must act like builtin filter
    assert to_list(filter(Stream(src), adapt(keep)), itype=itype) == list(
        builtins.filter(keep, src)
    )
    # `filter` with `bool` as predicate must act like builtin filter with None predicate.
    assert to_list(filter(Stream(src), adapt(bool)), itype=itype) == list(
        builtins.filter(None, src)
    )


@pytest.mark.parametrize(
    "itype, skip, adapt",
    (
        (itype, skip, adapt)
        for skip, adapt in ((Stream.skip, identity), (Stream.askip, asyncify))
        for itype in ITERABLE_TYPES
    ),
)
def test_skip(itype: IterableType, skip, adapt) -> None:
    # `skip` must raise ValueError if `until` is negative
    with pytest.raises(ValueError, match="`until` must be >= 0 but got -1"):
        skip(Stream(src), -1)
    with pytest.raises(
        TypeError,
        match="`until` must be an int or a callable, but got ",
    ):
        skip(Stream(src), "")
    for count in [0, 1, 3]:
        # `skip` must skip `until` elements
        assert to_list(skip(Stream(src), count), itype=itype) == list(src)[count:]
        # `skip` should not count exceptions as skipped elements
        assert (
            to_list(
                skip(Stream(map(throw_for_odd_func(TestError), src)), count).catch(
                    TestError
                ),
                itype=itype,
            )
            == list(filter(lambda i: i % 2 == 0, src))[count:]
        )
        # `skip` must yield starting from the first element satisfying `until`
        assert (
            to_list(skip(Stream(src), until=adapt(lambda n: n >= count)), itype=itype)
            == list(src)[count:]
        )


@pytest.mark.parametrize(
    "itype, truncate, adapt",
    (
        (itype, truncate, adapt)
        for truncate, adapt in (
            (Stream.truncate, identity),
            (Stream.atruncate, asyncify),
        )
        for itype in ITERABLE_TYPES
    ),
)
def test_truncate(itype: IterableType, truncate, adapt) -> None:
    # `truncate` must be ok with `when` >= stream length
    assert to_list(truncate(Stream(src), N * 2), itype=itype) == list(src)
    # `truncate` must be ok with `when` >= 1
    assert to_list(truncate(Stream(src), 2), itype=itype) == [0, 1]
    # `truncate` must be ok with `when` == 1
    assert to_list(truncate(Stream(src), 1), itype=itype) == [0]
    # `truncate` must be ok with `when` == 0
    assert to_list(truncate(Stream(src), 0), itype=itype) == []
    # `truncate` must raise ValueError if `when` is negative
    with pytest.raises(
        ValueError,
        match="`when` must be >= 0 but got -1",
    ):
        truncate(Stream(src), -1)

    # `truncate` must be no-op if `when` greater than source's size
    assert to_list(truncate(Stream(src), sys.maxsize), itype=itype) == list(src)
    count = N // 2
    raising_stream_iterator = bi_iterable_to_iter(
        truncate(Stream(lambda: map(lambda x: round((1 / x) * x**2), src)), count),
        itype=itype,
    )
    # `truncate` should not stop iteration when encountering exceptions and raise them without counting them...
    with pytest.raises(ZeroDivisionError):
        anext_or_next(raising_stream_iterator)
    assert alist_or_list(raising_stream_iterator) == list(range(1, count + 1))
    # ... and after reaching the limit it still continues to raise StopIteration on calls to next
    with pytest.raises(stopiteration_for_iter_type(type(raising_stream_iterator))):
        anext_or_next(raising_stream_iterator)

    iter_truncated_on_predicate = bi_iterable_to_iter(
        truncate(Stream(src), when=adapt(lambda n: n == 5)), itype=itype
    )
    # `when` n == 5 must be equivalent to `when` = 5
    assert alist_or_list(iter_truncated_on_predicate) == to_list(
        truncate(Stream(src), 5), itype=itype
    )
    # After exhaustion a call to __next__ on a truncated iterator must raise StopIteration
    with pytest.raises(stopiteration_for_iter_type(type(iter_truncated_on_predicate))):
        anext_or_next(iter_truncated_on_predicate)
    # an exception raised by `when` must be raised
    with pytest.raises(ZeroDivisionError):
        to_list(truncate(Stream(src), when=adapt(lambda _: 1 / 0)), itype=itype)


@pytest.mark.parametrize(
    "itype, group, groupby, adapt, nostop_",
    (
        (itype, group, groupby, adapt, nostop_)
        for group, groupby, adapt, nostop_ in (
            (Stream.group, Stream.groupby, identity, nostop),
            (Stream.agroup, Stream.agroupby, asyncify, anostop),
        )
        for itype in ITERABLE_TYPES
    ),
)
def test_group(itype: IterableType, group, groupby, adapt, nostop_) -> None:
    # `group` should raise error when called with `seconds` <= 0.
    for seconds in [-1, 0]:
        with pytest.raises(ValueError):
            to_list(
                group(
                    Stream([1]), size=100, interval=datetime.timedelta(seconds=seconds)
                ),
                itype=itype,
            )

    # `group` should raise error when called with `size` < 1.
    for size in [-1, 0]:
        with pytest.raises(ValueError):
            to_list(group(Stream([1]), size=size), itype=itype)

    # group size
    assert to_list(group(Stream(range(6)), size=4), itype=itype) == [
        [0, 1, 2, 3],
        [4, 5],
    ]
    assert to_list(group(Stream(range(6)), size=2), itype=itype) == [
        [0, 1],
        [2, 3],
        [4, 5],
    ]
    assert to_list(group(Stream([]), size=2), itype=itype) == []

    # behavior with exceptions
    def f(i):
        return i / (110 - i)

    stream_iterator = bi_iterable_to_iter(
        group(Stream(lambda: map(f, src)), 100), itype=itype
    )
    anext_or_next(stream_iterator)
    # when encountering upstream exception, `group` should yield the current accumulated group...
    assert anext_or_next(stream_iterator) == list(map(f, range(100, 110)))
    # ... and raise the upstream exception during the next call to `next`...
    with pytest.raises(ZeroDivisionError):
        anext_or_next(stream_iterator)

    # ... and restarting a fresh group to yield after that.
    assert anext_or_next(stream_iterator) == list(map(f, range(111, 211)))

    # behavior of the `seconds` parameter
    # `group` should not yield empty groups even though `interval` if smaller than upstream's frequency
    assert to_list(
        group(
            Stream(lambda: map(slow_identity, src)),
            size=100,
            interval=datetime.timedelta(seconds=slow_identity_duration / 1000),
        ),
        itype=itype,
    ) == list(map(lambda e: [e], src))
    # `group` with `by` argument should not yield empty groups even though `interval` if smaller than upstream's frequency
    assert to_list(
        group(
            Stream(lambda: map(slow_identity, src)),
            size=100,
            interval=datetime.timedelta(seconds=slow_identity_duration / 1000),
            by=adapt(lambda _: None),
        ),
        itype=itype,
    ) == list(map(lambda e: [e], src))
    # `group` should yield upstream elements in a two-element group if `interval` inferior to twice the upstream yield period
    assert to_list(
        group(
            Stream(lambda: map(slow_identity, src)),
            size=100,
            interval=datetime.timedelta(seconds=2 * slow_identity_duration * 0.99),
        ),
        itype=itype,
    ) == list(map(lambda e: [e, e + 1], even_src))

    # `group` without arguments should group the elements all together
    assert anext_or_next(bi_iterable_to_iter(group(Stream(src)), itype=itype)) == list(
        src
    )

    groupby_stream_iter: Union[
        Iterator[Tuple[int, List[int]]], AsyncIterator[Tuple[int, List[int]]]
    ] = bi_iterable_to_iter(
        groupby(Stream(src), adapt(lambda n: n % 2), size=2), itype=itype
    )
    # `groupby` must cogroup elements.
    assert [
        anext_or_next(groupby_stream_iter),
        anext_or_next(groupby_stream_iter),
    ] == [(0, [0, 2]), (1, [1, 3])]

    # test by
    stream_iter = bi_iterable_to_iter(
        group(Stream(src), size=2, by=adapt(lambda n: n % 2)), itype=itype
    )
    # `group` called with a `by` function must cogroup elements.
    assert [anext_or_next(stream_iter), anext_or_next(stream_iter)] == [
        [0, 2],
        [1, 3],
    ]
    # `group` called with a `by` function and a `size` should yield the first batch becoming full.
    assert anext_or_next(
        bi_iterable_to_iter(
            group(
                Stream(src_raising_at_exhaustion),
                size=10,
                by=adapt(lambda n: n % 4 != 0),
            ),
            itype=itype,
        ),
    ) == [1, 2, 3, 5, 6, 7, 9, 10, 11, 13]
    # `group` called with a `by` function and an infinite size must cogroup elements and yield groups starting with the group containing the oldest element.
    assert to_list(group(Stream(src), by=adapt(lambda n: n % 2)), itype=itype) == [
        list(range(0, N, 2)),
        list(range(1, N, 2)),
    ]
    # `group` called with a `by` function and reaching exhaustion must cogroup elements and yield uncomplete groups starting with the group containing the oldest element, even though it's not the largest.
    assert to_list(
        group(Stream(range(10)), by=adapt(lambda n: n % 4 == 0)), itype=itype
    ) == [[0, 4, 8], [1, 2, 3, 5, 6, 7, 9]]

    stream_iter = bi_iterable_to_iter(
        group(Stream(src_raising_at_exhaustion), by=adapt(lambda n: n % 2)),
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
    # test seconds + by
    # `group` called with a `by` function must cogroup elements and yield the largest groups when `seconds` is reached event though it's not the oldest.
    assert to_list(
        group(
            Stream(lambda: map(slow_identity, range(10))),
            interval=datetime.timedelta(seconds=slow_identity_duration * 2.9),
            by=adapt(lambda n: n % 4 == 0),
        ),
        itype=itype,
    ) == [[1, 2], [0, 4], [3, 5, 6, 7], [8], [9]]

    stream_iter = bi_iterable_to_iter(
        group(
            Stream(src),
            size=3,
            by=nostop_(
                adapt(
                    lambda n: throw(stopiteration_for_iter_type(itype)) if n == 2 else n
                )
            ),
        ),
        itype=itype,
    )
    # `group` should yield incomplete groups when `by` raises
    assert [anext_or_next(stream_iter), anext_or_next(stream_iter)] == [[0], [1]]
    # `group` should raise and skip `elem` if `by(elem)` raises
    with pytest.raises(
        RuntimeError,
        match=stopiteration_for_iter_type(itype).__name__,
    ):
        anext_or_next(stream_iter)
    # `group` should continue yielding after `by`'s exception has been raised.
    assert anext_or_next(stream_iter) == [3]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_throttle(itype: IterableType) -> None:
    # `throttle` should raise error when called with negative `per`.
    with pytest.raises(
        ValueError,
        match=r"`per` must be None or a positive timedelta but got datetime\.timedelta\(0\)",
    ):
        to_list(
            Stream([1]).throttle(1, per=datetime.timedelta(microseconds=0)),
            itype=itype,
        )
        # `throttle` should raise error when called with `count` < 1.
    with pytest.raises(ValueError, match="`count` must be >= 1 but got 0"):
        to_list(Stream([1]).throttle(0, per=datetime.timedelta(seconds=1)), itype=itype)

    # test interval
    interval_seconds = 0.3
    super_slow_elem_pull_seconds = 2 * interval_seconds
    N = 10
    integers = range(N)

    def slow_first_elem(elem: int):
        if elem == 0:
            time.sleep(super_slow_elem_pull_seconds)
        return elem

    for stream, expected_elems in cast(
        List[Tuple[Stream, List]],
        [
            (
                Stream(map(slow_first_elem, integers)).throttle(
                    1, per=datetime.timedelta(seconds=interval_seconds)
                ),
                list(integers),
            ),
            (
                Stream(map(throw_func(TestError), map(slow_first_elem, integers)))
                .throttle(1, per=datetime.timedelta(seconds=interval_seconds))
                .catch(TestError),
                [],
            ),
        ],
    ):
        duration, res = timestream(stream, itype=itype)
        # `throttle` with `interval` must yield upstream elements
        assert res == expected_elems
        expected_duration = (N - 1) * interval_seconds + super_slow_elem_pull_seconds
        # avoid bursts after very slow particular upstream elements
        assert duration == pytest.approx(expected_duration, rel=0.1)
    # `throttle` should avoid 'ValueError: sleep length must be non-negative' when upstream is slower than `interval`
    assert (
        anext_or_next(
            bi_iterable_to_iter(
                Stream(src)
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
        for stream, expected_elems in cast(
            List[Tuple[Stream, List]],
            [
                (
                    Stream(integers).throttle(
                        per_second, per=datetime.timedelta(seconds=1)
                    ),
                    list(integers),
                ),
                (
                    Stream(map(throw_func(TestError), integers))
                    .throttle(per_second, per=datetime.timedelta(seconds=1))
                    .catch(TestError),
                    [],
                ),
            ],
        ):
            duration, res = timestream(stream, itype=itype)
            # `throttle` with `per_second` must yield upstream elements
            assert res == expected_elems
            expected_duration = math.ceil(N / per_second) - 1
            # `throttle` must slow according to `per_second`
            assert duration == pytest.approx(
                expected_duration, abs=0.01 * expected_duration + 0.01
            )

    # test chain

    expected_duration = 2
    for stream in [
        Stream(range(11))
        .throttle(5, per=datetime.timedelta(seconds=1))
        .throttle(1, per=datetime.timedelta(seconds=0.01)),
        Stream(range(11))
        .throttle(20, per=datetime.timedelta(seconds=1))
        .throttle(1, per=datetime.timedelta(seconds=0.2)),
    ]:
        duration, _ = timestream(stream, itype=itype)
        # `throttle` with both `per_second` and `interval` set should follow the most restrictive
        assert duration == pytest.approx(expected_duration, rel=0.1)


@pytest.mark.parametrize(
    "itype, distinct, adapt",
    (
        (itype, distinct, adapt)
        for distinct, adapt in (
            (Stream.distinct, identity),
            (Stream.adistinct, asyncify),
        )
        for itype in ITERABLE_TYPES
    ),
)
def test_distinct(itype: IterableType, distinct, adapt) -> None:
    # `distinct` should yield distinct elements
    assert to_list(distinct(Stream("abbcaabcccddd")), itype=itype) == list("abcd")
    # `distinct` should only remove the duplicates that are consecutive if `consecutive=True`
    assert to_list(
        distinct(Stream("aabbcccaabbcccc"), consecutive=True), itype=itype
    ) == list("abcabc")
    for consecutive in [True, False]:
        # `distinct` should yield the first encountered elem among duplicates
        assert to_list(
            distinct(
                Stream(["foo", "bar", "a", "b"]),
                adapt(len),
                consecutive=consecutive,
            ),
            itype=itype,
        ) == ["foo", "a"]
        # `distinct` should yield zero elements on empty stream
        assert to_list(distinct(Stream([]), consecutive=consecutive), itype=itype) == []
    # `distinct` should raise for non-hashable elements
    with pytest.raises(TypeError, match="unhashable type: 'list'"):
        to_list(distinct(Stream([[1]])), itype=itype)


@pytest.mark.parametrize(
    "itype, catch, adapt",
    (
        (itype, catch, adapt)
        for catch, adapt in ((Stream.catch, identity), (Stream.acatch, asyncify))
        for itype in ITERABLE_TYPES
    ),
)
def test_catch(itype: IterableType, catch, adapt) -> None:
    # `catch` should yield elements in exception-less scenarios
    assert to_list(
        catch(Stream(src), Exception, finally_raise=True), itype=itype
    ) == list(src)
    # `catch` should raise TypeError when first argument is not None or Type[Exception], or Iterable[Optional[Type[Exception]]]
    with pytest.raises(
        TypeError,
        match="`errors` must be an `Exception` subclass or a tuple of such classes but got 1",
    ):
        catch(Stream(src), 1)  # type: ignore

    def f(i):
        return i / (3 - i)

    stream = Stream(lambda: map(f, src))
    safe_src = list(src)
    del safe_src[3]
    # If the exception type matches the `error_type`, then the impacted element should be ignored.
    assert to_list(catch(stream, ZeroDivisionError), itype=itype) == list(
        map(f, safe_src)
    )
    # If a non-caught exception type occurs, then it should be raised.
    with pytest.raises(ZeroDivisionError):
        to_list(catch(stream, TestError), itype=itype)

    first_value = 1
    second_value = 2
    third_value = 3
    functions = [
        lambda: throw(TestError),
        lambda: throw(TypeError),
        lambda: first_value,
        lambda: second_value,
        lambda: throw(ValueError),
        lambda: third_value,
        lambda: throw(ZeroDivisionError),
    ]

    erroring_stream: Stream[int] = Stream(lambda: map(lambda f: f(), functions))
    for caught_erroring_stream in [
        catch(erroring_stream, Exception, finally_raise=True),
        catch(erroring_stream, Exception, finally_raise=True),
    ]:
        erroring_stream_iterator = bi_iterable_to_iter(
            caught_erroring_stream, itype=itype
        )
        # `catch` should yield the first non exception throwing element.
        assert anext_or_next(erroring_stream_iterator) == first_value
        n_yields = 1
        # `catch` should raise the first error encountered when `finally_raise` is True.
        with pytest.raises(TestError):
            while True:
                anext_or_next(erroring_stream_iterator)
                n_yields += 1
        # `catch` with `finally_raise`=True should finally raise StopIteration to avoid infinite recursion if there is another catch downstream.
        with pytest.raises(stopiteration_for_iter_type(type(erroring_stream_iterator))):
            anext_or_next(erroring_stream_iterator)
        # 3 elements should have passed been yielded between caught exceptions.
        assert n_yields == 3

    only_caught_errors_stream = catch(
        Stream(map(lambda _: throw(TestError), range(2000))),
        TestError,
    )
    # When upstream raise exceptions without yielding any element, listing the stream must return empty list, without recursion issue.
    assert to_list(only_caught_errors_stream, itype=itype) == []
    # When upstream raise exceptions without yielding any element, then the first call to `next` on a stream catching all errors should raise StopIteration.
    with pytest.raises(stopiteration_for_iter_type(itype)):
        anext_or_next(bi_iterable_to_iter(only_caught_errors_stream, itype=itype))

    iterator = bi_iterable_to_iter(
        catch(
            catch(
                Stream(map(throw, [TestError, ValueError])),
                ValueError,
                finally_raise=True,
            ),
            TestError,
            finally_raise=True,
        ),
        itype=itype,
    )
    # With 2 chained `catch`s with `finally_raise=True`, the error caught by the first `catch` is finally raised first (even though it was raised second)...
    with pytest.raises(ValueError):
        anext_or_next(iterator)
    # ... and then the error caught by the second `catch` is raised...
    with pytest.raises(TestError):
        anext_or_next(iterator)
    # ... and a StopIteration is raised next.
    with pytest.raises(stopiteration_for_iter_type(type(iterator))):
        anext_or_next(iterator)
    # `catch` does not catch if `when` not satisfied
    with pytest.raises(TypeError):
        to_list(
            catch(
                Stream(map(throw, [ValueError, TypeError])),
                Exception,
                when=adapt(lambda exception: "ValueError" in repr(exception)),
            ),
            itype=itype,
        )
    # `catch` should be able to yield a non-None replacement
    assert to_list(
        catch(
            Stream(map(lambda n: 1 / n, [0, 1, 2, 4])),
            ZeroDivisionError,
            replace=adapt(lambda e: float("inf")),
        ),
        itype=itype,
    ) == [float("inf"), 1, 0.5, 0.25]
    # `catch` should be able to yield a None replacement
    assert to_list(
        catch(
            Stream(map(lambda n: 1 / n, [0, 1, 2, 4])),
            ZeroDivisionError,
            replace=adapt(lambda e: None),
        ),
        itype=itype,
    ) == [None, 1, 0.5, 0.25]

    errors_counter: Counter[Type[Exception]] = Counter()
    # `catch` should accept multiple types
    assert to_list(
        catch(
            Stream(
                map(
                    lambda n: 1 / n,  # potential ZeroDivisionError
                    map(
                        throw_for_odd_func(TestError),  # potential TestError
                        map(
                            int,  # potential ValueError
                            "01234foo56789",
                        ),
                    ),
                )
            ),
            (ValueError, TestError, ZeroDivisionError),
            when=adapt(lambda err: errors_counter.update([type(err)]) is None),
        ),
        itype=itype,
    ) == list(map(lambda n: 1 / n, range(2, 10, 2)))
    # `catch` should accept multiple types
    assert errors_counter == {TestError: 5, ValueError: 3, ZeroDivisionError: 1}


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe(itype: IterableType) -> None:
    def inverse(chars: str) -> Stream[float]:
        return (
            Stream(chars)
            .map(int)
            .map(lambda n: 1 / n)
            .observe("inverses")
            .catch(ValueError)
        )

    # `observe` should yield upstream elements
    assert to_list(inverse("12---3456----7"), itype=itype) == list(
        map(lambda n: 1 / n, range(1, 8))
    )

    class Log(NamedTuple):
        errors: int
        yields: int

    logs: List[Log] = []
    with patch(
        "logging.Logger.info",
        lambda self, msg, dur, errors, yields: logs.append(Log(errors, yields)),
    ):
        # `observe` should reraise
        with pytest.raises(ZeroDivisionError):
            to_list(inverse("12---3456----07"), itype=itype)
        # `observe` errors and yields independently
        assert logs == [
            Log(errors=0, yields=1),
            Log(errors=0, yields=2),
            Log(errors=1, yields=2),
            Log(errors=2, yields=2),
            Log(errors=3, yields=4),
            Log(errors=4, yields=6),
            Log(errors=8, yields=6),
        ]

        logs.clear()
        to_list(inverse("12---3456----07").catch(ZeroDivisionError), itype=itype)
        # `observe` should produce one last log on StopIteration
        assert logs == [
            Log(errors=0, yields=1),
            Log(errors=0, yields=2),
            Log(errors=1, yields=2),
            Log(errors=2, yields=2),
            Log(errors=3, yields=4),
            Log(errors=4, yields=6),
            Log(errors=8, yields=6),
            Log(errors=8, yields=7),
        ]

        logs.clear()
        to_list(inverse("12---3456----0").catch(ZeroDivisionError), itype=itype)
        # `observe` should skip redundant last log on StopIteration
        assert logs == [
            Log(errors=0, yields=1),
            Log(errors=0, yields=2),
            Log(errors=1, yields=2),
            Log(errors=2, yields=2),
            Log(errors=3, yields=4),
            Log(errors=4, yields=6),
            Log(errors=8, yields=6),
        ]


def test_is_iterable() -> None:
    assert isinstance(Stream(src), Iterable)
    assert isinstance(Stream(src), AsyncIterable)


def test_count() -> None:
    acc: List[int] = []

    def effect(x: int) -> None:
        nonlocal acc
        acc.append(x)

    stream = Stream(lambda: map(effect, src))
    # `count` should return the count of elements.
    assert stream.count() == N
    # `count` should iterate over the entire stream.
    assert acc == list(src)


@pytest.mark.asyncio
async def test_acount() -> None:
    acc: List[int] = []

    def effect(x: int) -> None:
        nonlocal acc
        acc.append(x)

    stream = Stream(lambda: map(effect, src))
    # `count` should return the count of elements.
    assert (await stream.acount()) == N
    # `count` should iterate over the entire stream.
    assert acc == list(src)


def test_call() -> None:
    acc: List[int] = []
    stream = Stream(src).map(acc.append)
    # `__call__` should return the stream.
    assert stream() is stream
    # `__call__` should exhaust the stream.
    assert acc == list(src)


@pytest.mark.asyncio
async def test_await() -> None:
    acc: List[int] = []
    stream = Stream(src).map(acc.append)
    # __await__ should return the stream.
    assert (await awaitable_to_coroutine(stream)) is stream
    # __await__ should exhaust the stream.
    assert acc == list(src)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_multiple_iterations(itype: IterableType) -> None:
    stream = Stream(src)
    for _ in range(3):
        # The first iteration over a stream should yield the same elements as any subsequent iteration on the same stream, even if it is based on a `source` returning an iterator that only support 1 iteration.
        assert to_list(stream, itype=itype) == list(src)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_pipe(itype: IterableType) -> None:
    def func(
        stream: Stream, *ints: int, **strings: str
    ) -> Tuple[Stream, Tuple[int, ...], Dict[str, str]]:
        return stream, ints, strings

    stream = Stream(src)
    ints = (0, 1, 2, 3)
    strings = {"foo": "bar", "bar": "foo"}
    # `pipe` should pass the stream and args/kwargs to `func`.
    assert stream.pipe(func, *ints, **strings) == (stream, ints, strings)
    # `pipe` should be ok without args and kwargs.
    assert stream.pipe(to_list, itype=itype) == to_list(stream, itype=itype)


def test_eq() -> None:
    stream = (
        Stream(src)
        .catch((TypeError, ValueError), replace=identity, when=identity)
        .acatch((TypeError, ValueError), replace=async_identity, when=async_identity)
        .distinct(identity)
        .adistinct(async_identity)
        .filter(identity)
        .afilter(async_identity)
        .foreach(identity, concurrency=3)
        .aforeach(async_identity, concurrency=3)
        .group(3, by=bool)
        .flatten(concurrency=3)
        .agroup(3, by=async_identity)
        .map(iter)
        .map(sync_to_async_iter)
        .aflatten(concurrency=3)
        .groupby(bool)
        .agroupby(async_identity)
        .map(identity, via="process")
        .amap(async_identity)
        .observe("foo")
        .skip(3)
        .askip(3)
        .truncate(4)
        .atruncate(4)
        .throttle(1, per=datetime.timedelta(seconds=1))
    )

    assert stream == (
        Stream(src)
        .catch((TypeError, ValueError), replace=identity, when=identity)
        .acatch((TypeError, ValueError), replace=async_identity, when=async_identity)
        .distinct(identity)
        .adistinct(async_identity)
        .filter(identity)
        .afilter(async_identity)
        .foreach(identity, concurrency=3)
        .aforeach(async_identity, concurrency=3)
        .group(3, by=bool)
        .flatten(concurrency=3)
        .agroup(3, by=async_identity)
        .map(iter)
        .map(sync_to_async_iter)
        .aflatten(concurrency=3)
        .groupby(bool)
        .agroupby(async_identity)
        .map(identity, via="process")
        .amap(async_identity)
        .observe("foo")
        .skip(3)
        .askip(3)
        .truncate(4)
        .atruncate(4)
        .throttle(1, per=datetime.timedelta(seconds=1))
    )
    assert stream != (
        Stream(list(src))  # not same source
        .catch((TypeError, ValueError), replace=lambda e: 2, when=identity)
        .acatch(
            (TypeError, ValueError), replace=asyncify(lambda e: 2), when=async_identity
        )
        .distinct(identity)
        .adistinct(async_identity)
        .filter(identity)
        .afilter(async_identity)
        .foreach(identity, concurrency=3)
        .aforeach(async_identity, concurrency=3)
        .group(3, by=bool)
        .flatten(concurrency=3)
        .agroup(3, by=async_identity)
        .map(iter)
        .map(sync_to_async_iter)
        .aflatten(concurrency=3)
        .groupby(bool)
        .agroupby(async_identity)
        .map(identity, via="process")
        .amap(async_identity)
        .observe("foo")
        .skip(3)
        .askip(3)
        .truncate(4)
        .atruncate(4)
        .throttle(1, per=datetime.timedelta(seconds=1))
    )
    assert stream != (
        Stream(src)
        .catch((TypeError, ValueError), replace=lambda e: 2, when=identity)
        .acatch(
            (TypeError, ValueError), replace=asyncify(lambda e: 2), when=async_identity
        )
        .distinct(identity)
        .adistinct(async_identity)
        .filter(identity)
        .afilter(async_identity)
        .foreach(identity, concurrency=3)
        .aforeach(async_identity, concurrency=3)
        .group(3, by=bool)
        .flatten(concurrency=3)
        .agroup(3, by=async_identity)
        .map(iter)
        .map(sync_to_async_iter)
        .aflatten(concurrency=3)
        .groupby(bool)
        .agroupby(async_identity)
        .map(identity, via="process")
        .amap(async_identity)
        .observe("foo")
        .skip(3)
        .askip(3)
        .truncate(4)
        .atruncate(4)
        .throttle(1, per=datetime.timedelta(seconds=2))  # not the same interval
    )


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_ref_cycles(itype: IterableType) -> None:
    async def async_int(o: Any) -> int:
        return int(o)

    stream = (
        Stream("123_5")
        .amap(async_int)
        .map(str)
        .group(1)
        .groupby(len)
        .catch(Exception, finally_raise=True)
    )
    exception: Exception
    try:
        to_list(stream, itype=itype)
    except ValueError as e:
        exception = e
    # `finally_raise` must be respected
    assert isinstance(exception, ValueError)
    # the exception's traceback should not contain an exception captured in its own traceback
    assert [
        (var, val)
        # go through the frames of the exception's traceback
        for frame, _ in traceback.walk_tb(exception.__traceback__)
        # skipping the current frame
        if frame is not cast(TracebackType, exception.__traceback__).tb_frame
        # go through the locals captured in that frame
        for var, val in frame.f_locals.items()
        # check if one of them is an exception
        if isinstance(val, Exception)
        # check if it is captured in its own traceback
        and frame is cast(TracebackType, val.__traceback__).tb_frame
    ] == []


def test_on_queue_in_thread() -> None:
    zeros: List[str] = []
    src: "queue.Queue[Optional[str]]" = queue.Queue()
    thread = threading.Thread(target=Stream(iter(src.get, None)).foreach(zeros.append))
    thread.start()
    src.put("foo")
    src.put("bar")
    src.put(None)
    thread.join()
    # stream must work on Queue
    assert zeros == ["foo", "bar"]


def test_deepcopy() -> None:
    stream = Stream([]).map(str)
    stream_copy = copy.deepcopy(stream)
    # the copy must be equal
    assert stream == stream_copy
    # the copy must be a different object
    assert stream is not stream_copy
    # the copy's source must be a different object
    assert stream.source is not stream_copy.source


def test_slots() -> None:
    stream = Stream(src).filter(bool)
    # a stream should not have a __dict__
    with pytest.raises(AttributeError):
        Stream(src).__dict__
    # a stream should have __slots__
    assert stream.__slots__ == ("_where",)
    # a stream should not have a __dict__
    with pytest.raises(AttributeError):
        stream.__dict__


def test_iter_loop_auto_closing() -> None:
    original_new_event_loop = asyncio.new_event_loop
    created_loop: "queue.Queue[asyncio.AbstractEventLoop]" = queue.Queue(maxsize=1)

    def tracking_new_event_loop():
        loop = original_new_event_loop()
        created_loop.put_nowait(loop)
        return loop

    asyncio.new_event_loop = tracking_new_event_loop
    iterator_a = iter(Stream(src).afilter(async_identity))
    loop_a = created_loop.get_nowait()
    iterator_b = iter(Stream(src).afilter(async_identity))
    loop_b = created_loop.get_nowait()
    # iterator_a is not deleted, its loop should not be closed
    assert not loop_a.is_closed()
    # iterator_b is not deleted, its loop should not be closed
    assert not loop_b.is_closed()
    del iterator_a
    # iterator_a is deleted, its loop should be closed
    assert loop_a.is_closed()
    # iterator_b is not deleted, its loop should not be closed
    assert not loop_b.is_closed()
    del iterator_b
    # iterator_b is deleted, its loop should be closed
    assert loop_b.is_closed()
    asyncio.new_event_loop = original_new_event_loop


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stream",
    (
        Stream(range(N)).map(slow_identity, concurrency=N // 8),
        (
            Stream(range(N))
            .map(lambda i: map(slow_identity, (i,)))
            .flatten(concurrency=N // 8)
        ),
    ),
)
async def test_run_in_executor(stream: Stream) -> None:
    """
    Tests that executor-based concurrent mapping/flattening are wrapped
    in non-loop-blocking run_in_executor-based async tasks.
    """
    concurrency = N // 8
    res: tuple[int, int]
    duration, res = await timecoro(
        lambda: asyncio.gather(stream.acount(), stream.acount()), times=10
    )
    assert tuple(res) == (N, N)
    assert duration == pytest.approx(N * slow_identity_duration / concurrency, rel=0.25)
