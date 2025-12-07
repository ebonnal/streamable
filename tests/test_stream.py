import asyncio
import builtins
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
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

from streamable import stream
from streamable._utils._async import awaitable_to_coroutine
from streamable._utils._func import anostop, asyncify, nostop, star
from streamable._utils._iter import sync_to_async_iter, sync_to_bi_iterable
from tests.utils import (
    ITERABLE_TYPES,
    IterableType,
    N,
    to_async_iter,
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
    ints_src,
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
    ints = stream(ints_src)
    # The stream's `source` must be the source argument.
    assert ints._source is ints_src
    # "The `upstream` attribute of a base Stream's instance must be None."
    assert ints.upstream is None
    # `source` must be propagated by operations
    assert (
        stream(ints_src)
        .group(100)
        .flatten()
        .map(identity)
        .map(async_identity)
        .do(identity)
        .do(async_identity)
        .catch(Exception)
        .observe()
        .throttle(1, per=datetime.timedelta(seconds=1))
        .source
    ) is ints_src
    # attribute `source` must be read-only
    with pytest.raises(AttributeError):
        stream(ints_src).source = ints_src  # type: ignore
    # attribute `upstream` must be read-only
    with pytest.raises(AttributeError):
        stream(ints_src).upstream = stream(ints_src)  # type: ignore


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_async_src(itype) -> None:
    # a stream with an async source must be collectable as an Iterable or as AsyncIterable
    assert to_list(stream(sync_to_async_iter(iter(ints_src))), itype) == list(ints_src)
    # a stream with an async source must be collectable as an Iterable or as AsyncIterable
    assert to_list(stream(sync_to_async_iter(iter(ints_src)).__aiter__), itype) == list(
        ints_src
    )


def test_repr(complex_stream: stream, complex_stream_str: str) -> None:
    print_stream = stream([]).map(star(print))
    assert (
        repr(print_stream)
        == "stream([]).map(star(<built-in function print>), concurrency=1, ordered=True)"
    )
    assert (
        str(print_stream) == "stream([]).map(star(print), concurrency=1, ordered=True)"
    )
    # `repr` should work as expected on a stream with many operation
    assert str(complex_stream) == complex_stream_str
    # explanation of different streams must be different
    assert str(complex_stream) != str(complex_stream.map(str))
    # `repr` should work as expected on a stream without operation
    assert str(stream(ints_src)) == "stream(range(0, 256))"
    # `repr` should return a one-liner for a stream with 1 operations
    assert str(stream(ints_src).skip(10)) == "stream(range(0, 256)).skip(until=10)"
    # `repr` should return a one-liner for a stream with 2 operations
    assert (
        str(stream(ints_src).skip(10).skip(10))
        == "stream(range(0, 256)).skip(until=10).skip(until=10)"
    )
    # `repr` should go to line if it exceeds than 80 chars
    assert (
        str(stream(ints_src).skip(10).skip(10).skip(10).skip(10))
        == """(
    stream(range(0, 256))
    .skip(until=10)
    .skip(until=10)
    .skip(until=10)
    .skip(until=10)
)"""
    )


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_iter(itype: IterableType) -> None:
    # iter(stream) must return an Iterator.
    assert isinstance(bi_iterable_to_iter(stream(ints_src), itype=itype), itype)
    # Getting an Iterator from a Stream with a source not being a Union[Callable[[], Iterator], ITerable] must raise TypeError.
    with pytest.raises(
        TypeError,
        match=r"`source` must be Iterable or AsyncIterable or Callable but got <class 'int'>",
    ):
        bi_iterable_to_iter(stream(1), itype=itype)  # type: ignore
    # Getting an Iterator from a Stream with a source not being a Union[Callable[[], Iterator], ITerable] must raise TypeError.
    with pytest.raises(
        TypeError,
        match=r"if `source` is callable it must return an Iterable or AsyncIterable but got <class 'int'>",
    ):
        bi_iterable_to_iter(stream(lambda: 1), itype=itype)  # type: ignore


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_add(itype: IterableType) -> None:
    from streamable._stream import FlattenStream

    ints = stream(ints_src)
    # stream addition must return a FlattenStream.
    assert isinstance(ints + ints, FlattenStream)

    stream_a = stream(range(10))
    stream_b = stream(range(10, 20))
    stream_c = stream(range(20, 30))
    # `chain` must yield the elements of the first stream the move on with the elements of the next ones and so on.
    assert to_list(stream_a + stream_b + stream_c, itype=itype) == list(range(30))

    stream_ = stream(range(10))
    stream_ += stream(range(10, 20))
    stream_ += stream(range(20, 30))
    # `chain` must yield the elements of the first stream the move on with the elements of the next ones and so on.
    assert to_list(stream_, itype=itype) == list(range(30))


@pytest.mark.parametrize(
    "concurrency, itype",
    [(concurrency, itype) for concurrency in (1, 2) for itype in ITERABLE_TYPES],
)
def test_map(concurrency, itype) -> None:
    # At any concurrency the `map` method should act as the builtin map function, transforming elements while preserving input elements order.
    assert to_list(
        stream(ints_src).map(randomly_slowed(square), concurrency=concurrency),
        itype=itype,
    ) == list(map(square, ints_src))


@pytest.mark.parametrize(
    "operation, fn_name",
    [
        (stream.do, "effect"),
        (stream.map, "into"),
    ],
)
def test_executor_concurrency_with_async_function(operation, fn_name):
    with pytest.raises(
        TypeError,
        match=f"`concurrency` must be an int if `{fn_name}` is a coroutine function but got <concurrent.futures.thread.ThreadPoolExecutor object at .*",
    ):
        operation(
            stream(ints_src),
            async_identity_sleep,
            concurrency=ThreadPoolExecutor(max_workers=2),
        )


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

    with ProcessPoolExecutor(max_workers=10) as processes:
        sleeps = [0.01, 1, 0.01]
        state: List[str] = []
        expected_result_list: List[str] = list(order_mutation(map(str, sleeps)))
        stream_ = (
            stream(sleeps)
            .do(identity_sleep, concurrency=processes, ordered=ordered)
            .map(str, concurrency=processes, ordered=True)
            .do(state.append, concurrency=processes, ordered=True)
            .do(lambda _: state.append(""), concurrency=1, ordered=True)
        )
        # process-based concurrency must correctly transform elements, respecting `ordered`...
        assert to_list(stream_, itype=itype) == expected_result_list
        # ... and should not mutate main thread-bound structures.
        assert state == [""] * len(sleeps)

        if sys.version_info >= (3, 9):
            for f in [lambda x: x, local_identity]:
                # process-based concurrency should not be able to serialize a lambda or a local func
                with pytest.raises(
                    (AttributeError, PickleError),
                    match="<locals>",
                ):
                    to_list(stream(ints_src).map(f, concurrency=processes), itype=itype)
            # partial iteration
            assert (
                anext_or_next(bi_iterable_to_iter(stream_, itype=itype))
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
        stream(range(n_elems)).map(str, concurrency=concurrency), itype=itype
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
    # at any concurrency, map/do should not stop iteration when upstream raises
    assert (
        to_list(
            stream([0, 1, 0, 2, 0])
            .map(lambda n: 1 / n)
            .map(identity_sleep, concurrency=concurrency, ordered=ordered)
            .catch(ZeroDivisionError, replace=lambda e: float("inf")),
            itype=itype,
        )
        == expected
    )
    # at any concurrency, async map/do should not stop iteration when upstream raises
    assert (
        to_list(
            stream([0, 1, 0, 2, 0])
            .map(lambda n: 1 / n)
            .map(async_identity_sleep, concurrency=concurrency, ordered=ordered)
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
            (stream.do, time.sleep),
            (stream.map, identity_sleep),
            (stream.do, asyncio.sleep),
            (stream.map, async_identity_sleep),
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
        operation(stream(seconds), func, ordered=ordered, concurrency=2),
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
def test_do(concurrency, itype) -> None:
    side_collection: Set[int] = set()

    def side_effect(x: int, func: Callable[[int], int]):
        nonlocal side_collection
        side_collection.add(func(x))

    res = to_list(
        stream(ints_src).do(
            lambda i: randomly_slowed(side_effect(i, square)),
            concurrency=concurrency,
        ),
        itype=itype,
    )
    # At any concurrency the `do` method should return the upstream elements in order.
    assert res == list(ints_src)
    # At any concurrency the `do` method should call func on upstream elements (in any order).
    assert side_collection == set(map(square, ints_src))


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
            (stream.do, throw_func, throw_for_odd_func, nostop),
            (stream.do, async_throw_func, async_throw_for_odd_func, anostop),
            (stream.map, throw_func, throw_for_odd_func, nostop),
            (stream.map, async_throw_func, async_throw_for_odd_func, anostop),
        ]
        for itype in ITERABLE_TYPES
    ],
)
def test_map_or_do_with_exception(
    raised_exc: Type[Exception],
    caught_exc: Type[Exception],
    concurrency: int,
    method: Callable[[stream, Callable[[Any], int], int], stream],
    throw_func: Callable[[Type[Exception]], Callable[[Any], int]],
    throw_for_odd_func: Callable[[Type[Exception]], Callable[[Any], int]],
    nostop: Callable[[Any], Callable[[Any], int]],
    itype: IterableType,
) -> None:
    rasing_stream: stream[int] = method(
        stream(iter(ints_src)), nostop(throw_func(raised_exc)), concurrency=concurrency
    )  # type: ignore
    # At any concurrency, `map` and `do` must raise.
    with pytest.raises(caught_exc):
        to_list(rasing_stream, itype=itype)
    # Only `concurrency` upstream elements should be initially pulled for processing (0 if `concurrency=1`), and 1 more should be pulled for each call to `next`.
    assert next(cast(Iterator[int], rasing_stream.source)) == (
        concurrency + 1 if concurrency > 1 else concurrency
    )
    # At any concurrency, `map` and `do` should not stop after one exception occured.
    assert to_list(
        method(
            stream(ints_src),
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
            (stream.do, slow_identity),
            (stream.do, async_slow_identity),
            (stream.map, slow_identity),
            (stream.map, async_slow_identity),
        ]
        for concurrency in [1, 2, 4]
        for itype in ITERABLE_TYPES
    ],
)
def test_map_or_do_concurrency(method, func, concurrency, itype) -> None:
    expected_iteration_duration = N * slow_identity_duration / concurrency
    duration, res = timestream(
        method(stream(ints_src), func, concurrency=concurrency), itype=itype
    )
    assert res == list(ints_src)
    # Increasing the concurrency of mapping should decrease proportionnally the iteration's duration.
    assert duration == pytest.approx(expected_iteration_duration, rel=0.15)


@pytest.mark.parametrize(
    "concurrency, itype",
    [(concurrency, itype) for concurrency in (1, 100) for itype in ITERABLE_TYPES],
)
def test_map_async(concurrency, itype) -> None:
    # At any concurrency the `map` method should act as the builtin map function with async transforms, preserving input order.
    assert to_list(
        stream(ints_src).map(
            async_randomly_slowed(async_square), concurrency=concurrency
        ),
        itype=itype,
    ) == list(map(square, ints_src))


@pytest.mark.parametrize(
    "concurrency, itype",
    [(concurrency, itype) for concurrency in (1, 100) for itype in ITERABLE_TYPES],
)
def test_do_async(concurrency, itype) -> None:
    # At any concurrency the `do` method must preserve input elements order.
    assert to_list(
        stream(ints_src).do(
            async_randomly_slowed(async_square), concurrency=concurrency
        ),
        itype=itype,
    ) == list(ints_src)


def test_flatten_typing() -> None:
    flattened_iterator_stream: stream[str] = stream("abc").map(iter).flatten()  # noqa: F841
    flattened_list_stream: stream[str] = stream("abc").map(list).flatten()  # noqa: F841
    flattened_set_stream: stream[str] = stream("abc").map(set).flatten()  # noqa: F841
    flattened_map_stream: stream[str] = (  # noqa: F841
        stream("abc").map(lambda char: map(lambda x: x, char)).flatten()
    )
    flattened_filter_stream: stream[str] = (  # noqa: F841
        stream("abc").map(lambda char: filter(lambda _: True, char)).flatten()
    )

    flattened_asynciter_stream: stream[str] = (  # noqa: F841
        stream("abc").map(iter).map(sync_to_async_iter).flatten()
    )
    flattened_range_stream: stream[int] = (  # noqa: F841
        stream((ints_src, ints_src)).flatten()
    )


@pytest.mark.parametrize(
    "concurrency, itype, to_iter",
    [
        (concurrency, itype, to_iter)
        for concurrency in (1, 2)
        for itype in ITERABLE_TYPES
        for to_iter in (identity, to_async_iter)
    ],
)
def test_flatten(concurrency, itype, to_iter) -> None:
    n_iterables = 32
    it = list(range(N // n_iterables))
    double_it = it + it
    iterables_stream = stream(
        [sync_to_bi_iterable(double_it)]
        + [sync_to_bi_iterable(it) for _ in range(n_iterables)]
    )
    if concurrency == 1:
        # At concurrency == 1, `flatten` method should yield all the upstream iterables' elements in the order of a nested for loop.
        assert to_list(
            iterables_stream.map(to_iter).flatten(concurrency=concurrency),
            itype=itype,
        ) == [elem for iterable in iterables_stream for elem in iterable]
    else:
        # At concurrency > 1, the `flatten` method should yield all the upstream iterables' elements.
        assert Counter(
            to_list(
                iterables_stream.map(to_iter).flatten(concurrency=concurrency),
                itype=itype,
            )
        ) == Counter(list(it) * n_iterables + double_it)

    # At any concurrency the `flatten` method should continue flattening even if an iterable' __next__ raises an exception.
    assert to_list(
        stream([[4, 3, 2, 0], [1, 0, -1], [0, -2, -3]])
        .map(lambda iterable: sync_to_bi_iterable(map(lambda n: 1 / n, iterable)))
        .map(to_iter)
        .flatten(
            concurrency=concurrency,
        )
        .catch(ZeroDivisionError, replace=lambda e: float("inf")),
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
        stream([[4, 3, 2], cast(List[int], []), [1, 0]])
        .do(lambda ints: 1 / len(ints))
        .map(sync_to_bi_iterable)
        .map(to_iter)
        .flatten(
            concurrency=concurrency,
        )
        .catch(ZeroDivisionError, replace=lambda e: -1),
        itype=itype,
    ) == ([4, 3, 2, -1, 1, 0] if concurrency == 1 else [4, -1, 3, 1, 2, 0])
    # At any concurrency the `flatten` method should continue pulling upstream iterables even if upstream's __iter__ raises an exception.
    assert to_list(
        stream(
            [
                sync_to_bi_iterable([4, 3, 2]),
                cast(List[int], None),
                sync_to_bi_iterable([1, 0]),
            ]
        )
        .map(to_iter)
        .flatten(
            concurrency=concurrency,
        )
        .catch(AttributeError, replace=lambda e: -1),
        itype=itype,
    ) == ([4, 3, 2, -1, 1, 0] if concurrency == 1 else [4, -1, 3, 1, 2, 0])
    # `flatten` should not yield any element if upstream elements are empty iterables, and be resilient to recursion issue in case of successive empty upstream iterables.
    assert (
        to_list(
            stream([sync_to_bi_iterable(iter([])) for _ in range(2000)])
            .map(to_iter)
            .flatten(
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
                stream(cast(Union[Iterable, AsyncIterable], ints_src))
                .map(to_iter)
                .flatten(),
                itype=itype,
            )
        )


@pytest.mark.parametrize(
    "itype, concurrency",
    [(itype, concurrency) for itype in ITERABLE_TYPES for concurrency in (1, 2)],
)
def test_flatten_heterogeneous_sync_async_elements(itype, concurrency) -> None:
    async def aiterator() -> AsyncIterator[int]:
        yield 0
        yield 1

    def iterator() -> Iterator[int]:
        yield 0
        yield 1

    assert to_list(
        stream(
            cast(
                List[Union[AsyncIterator, Iterator]],
                [aiterator(), iterator(), aiterator(), iterator()],
            )
        ).flatten(concurrency=concurrency),
        itype=itype,
    ) == ([0, 1, 0, 1, 0, 1, 0, 1] if concurrency == 1 else [0, 0, 1, 1, 0, 0, 1, 1])


@pytest.mark.parametrize(
    "itype, slow, to_iter",
    [
        (itype, slow, to_iter)
        for slow, to_iter in (
            (partial(stream.map, into=slow_identity), stream.__iter__),
            (partial(stream.map, into=async_slow_identity), stream.__aiter__),
        )
        for itype in ITERABLE_TYPES
    ],
)
def test_flatten_concurrency(itype, slow, to_iter) -> None:
    concurrency = 2
    iterable_size = 5
    runtime, res = timestream(
        stream(
            lambda: [
                slow(stream(["a"] * iterable_size)),
                slow(stream(["b"] * iterable_size)),
                slow(stream(["c"] * iterable_size)),
            ]
        )
        .map(to_iter)
        .flatten(
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
    assert runtime == pytest.approx(expected_runtime, rel=0.15)


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
        for elem in ints_src:
            yielded_elems.append(elem)
            yield elem

    for stream_, n_pulls_after_first_next in [
        (
            stream(remembering_src).map(identity, concurrency=concurrency),
            concurrency + 1,
        ),
        (
            stream(remembering_src).map(async_identity, concurrency=concurrency),
            concurrency + 1,
        ),
        (
            stream(remembering_src).do(identity, concurrency=concurrency),
            concurrency + 1,
        ),
        (
            stream(remembering_src).do(async_identity, concurrency=concurrency),
            concurrency + 1,
        ),
        (
            stream(remembering_src).group(1).flatten(concurrency=concurrency),
            concurrency,
        ),
    ]:
        yielded_elems = []
        iterator = bi_iterable_to_iter(stream_, itype=itype)
        time.sleep(0.5)
        # before the first call to `next` a concurrent stream should have pulled 0 upstream elements.
        assert len(yielded_elems) == 0
        anext_or_next(iterator)
        time.sleep(0.5)
        # `after the first call to `next` a concurrent stream with given concurrency should have pulled only `n_pulls_after_first_next` upstream elements.
        assert len(yielded_elems) == n_pulls_after_first_next


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
def test_truncate(itype: IterableType, adapt) -> None:
    # `truncate` must be ok with `when` >= stream length
    assert to_list(stream(ints_src).truncate(N * 2), itype=itype) == list(ints_src)
    # `truncate` must be ok with `when` >= 1
    assert to_list(stream(ints_src).truncate(2), itype=itype) == [0, 1]
    # `truncate` must be ok with `when` == 1
    assert to_list(stream(ints_src).truncate(1), itype=itype) == [0]
    # `truncate` must be ok with `when` == 0
    assert to_list(stream(ints_src).truncate(0), itype=itype) == []
    # `truncate` must raise ValueError if `when` is negative
    with pytest.raises(
        ValueError,
        match="`when` must be >= 0 but got -1",
    ):
        stream(ints_src).truncate(-1)

    # `truncate` must be no-op if `when` greater than source's size
    assert to_list(stream(ints_src).truncate(sys.maxsize), itype=itype) == list(
        ints_src
    )
    count = N // 2
    raising_stream_iterator = bi_iterable_to_iter(
        stream(lambda: map(lambda x: round((1 / x) * x**2), ints_src)).truncate(count),
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
        stream(ints_src).truncate(adapt(lambda n: n == 5)), itype=itype
    )
    # `when` n == 5 must be equivalent to `when` = 5
    assert alist_or_list(iter_truncated_on_predicate) == to_list(
        stream(ints_src).truncate(5), itype=itype
    )
    # After exhaustion a call to __next__ on a truncated iterator must raise StopIteration
    with pytest.raises(stopiteration_for_iter_type(type(iter_truncated_on_predicate))):
        anext_or_next(iter_truncated_on_predicate)
    # an exception raised by `when` must be raised
    with pytest.raises(ZeroDivisionError):
        to_list(stream(ints_src).truncate(adapt(lambda _: 1 / 0)), itype=itype)


@pytest.mark.parametrize(
    "itype, adapt, nostop_",
    (
        (itype, adapt, nostop_)
        for adapt, nostop_ in ((identity, nostop), (asyncify, anostop))
        for itype in ITERABLE_TYPES
    ),
)
def test_group(itype: IterableType, adapt, nostop_) -> None:
    # `group` should raise error when called with `seconds` <= 0.
    for seconds in [-1, 0]:
        with pytest.raises(
            ValueError,
            match="`over` must be a positive timedelta but got datetime\.timedelta(.*)",
        ):
            to_list(
                stream([1]).group(up_to=100, over=datetime.timedelta(seconds=seconds)),
                itype=itype,
            )
            to_list(
                stream([1]).groupby(
                    str, up_to=100, over=datetime.timedelta(seconds=seconds)
                ),
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
        stream(lambda: map(f, ints_src)).group(100), itype=itype
    )
    anext_or_next(stream_iterator)
    # when encountering upstream exception, `group` should yield the current accumulated group...
    assert anext_or_next(stream_iterator) == list(map(f, range(100, 110)))
    # ... and raise the upstream exception during the next call to `next`...
    with pytest.raises(ZeroDivisionError):
        anext_or_next(stream_iterator)

    # ... and restarting a fresh group to yield after that.
    assert anext_or_next(stream_iterator) == list(map(f, range(111, 211)))

    # behavior of the `over` parameter
    # `group` should not yield empty groups even though `over` if smaller than upstream's frequency
    assert to_list(
        stream(lambda: map(slow_identity, ints_src)).group(
            up_to=100,
            over=datetime.timedelta(seconds=slow_identity_duration / 1000),
        ),
        itype=itype,
    ) == list(map(lambda e: [e], ints_src))
    # `group` with `by` argument should not yield empty groups even though `over` if smaller than upstream's frequency
    assert to_list(
        stream(lambda: map(slow_identity, ints_src)).group(
            up_to=100,
            over=datetime.timedelta(seconds=slow_identity_duration / 1000),
            by=adapt(lambda _: None),
        ),
        itype=itype,
    ) == list(map(lambda e: [e], ints_src))
    # `group` should yield upstream elements in a two-element group if `over` inferior to twice the upstream yield period
    assert to_list(
        stream(lambda: map(slow_identity, ints_src)).group(
            up_to=100,
            over=datetime.timedelta(seconds=2 * slow_identity_duration * 0.99),
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
        stream(ints_src).groupby(adapt(lambda n: n % 2), up_to=2), itype=itype
    )
    # `groupby` must cogroup elements.
    assert [
        anext_or_next(groupby_stream_iter),
        anext_or_next(groupby_stream_iter),
    ] == [(0, [0, 2]), (1, [1, 3])]

    # test by
    stream_iter = bi_iterable_to_iter(
        stream(ints_src).group(up_to=2, by=adapt(lambda n: n % 2)), itype=itype
    )
    # `group` called with a `by` function must cogroup elements.
    assert [anext_or_next(stream_iter), anext_or_next(stream_iter)] == [
        [0, 2],
        [1, 3],
    ]
    # `group` called with a `by` function and a `up_to` should yield the first batch becoming full.
    assert anext_or_next(
        bi_iterable_to_iter(
            stream(src_raising_at_exhaustion).group(
                up_to=10,
                by=adapt(lambda n: n % 4 != 0),
            ),
            itype=itype,
        ),
    ) == [1, 2, 3, 5, 6, 7, 9, 10, 11, 13]
    # `group` called with a `by` function and an infinite size must cogroup elements and yield groups starting with the group containing the oldest element.
    assert to_list(stream(ints_src).group(by=adapt(lambda n: n % 2)), itype=itype) == [
        list(range(0, N, 2)),
        list(range(1, N, 2)),
    ]
    # `group` called with a `by` function and reaching exhaustion must cogroup elements and yield uncomplete groups starting with the group containing the oldest element, even though it's not the largest.
    assert to_list(
        stream(range(10)).group(by=adapt(lambda n: n % 4 == 0)), itype=itype
    ) == [[0, 4, 8], [1, 2, 3, 5, 6, 7, 9]]

    stream_iter = bi_iterable_to_iter(
        stream(src_raising_at_exhaustion).group(by=adapt(lambda n: n % 2)),
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
        stream(lambda: map(slow_identity, range(10))).group(
            over=datetime.timedelta(seconds=slow_identity_duration * 2.9),
            by=adapt(lambda n: n % 4 == 0),
        ),
        itype=itype,
    ) == [[1, 2], [0, 4], [3, 5, 6, 7], [8], [9]]

    stream_iter = bi_iterable_to_iter(
        stream(ints_src).group(
            up_to=3,
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


@pytest.mark.parametrize(
    "itype, adapt",
    ((itype, adapt) for adapt in (identity, asyncify) for itype in ITERABLE_TYPES),
)
def test_catch(itype: IterableType, adapt) -> None:
    # `catch` should yield elements in exception-less scenarios
    assert to_list(
        stream(ints_src).catch(Exception, finally_raise=True), itype=itype
    ) == list(ints_src)
    with pytest.raises(
        TypeError,
        match="`when`/`replace`/`do` must all be coroutine functions or neither should be",
    ):
        to_list(
            stream(ints_src).catch(Exception, when=identity, replace=async_identity),
            itype=itype,
        )  # type: ignore

    def fn(i):
        return i / (3 - i)

    stream_ = stream(lambda: map(fn, ints_src))
    safe_src = list(ints_src)
    del safe_src[3]
    # If the exception type matches the `error_type`, then the impacted element should be ignored.
    assert to_list(stream_.catch(ZeroDivisionError), itype=itype) == list(
        map(fn, safe_src)
    )
    # If a non-caught exception type occurs, then it should be raised.
    with pytest.raises(ZeroDivisionError):
        to_list(stream_.catch(TestError), itype=itype)

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

    erroring_stream: stream[int] = stream(lambda: map(lambda f: f(), functions))
    for caught_erroring_stream in [
        erroring_stream.catch(Exception, finally_raise=True),
        erroring_stream.catch(Exception, finally_raise=True),
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

    only_caught_errors_stream = stream(
        map(lambda _: throw(TestError), range(2000))
    ).catch(TestError)
    # When upstream raise exceptions without yielding any element, listing the stream must return empty list, without recursion issue.
    assert to_list(only_caught_errors_stream, itype=itype) == []
    # When upstream raise exceptions without yielding any element, then the first call to `next` on a stream catching all errors should raise StopIteration.
    with pytest.raises(stopiteration_for_iter_type(itype)):
        anext_or_next(bi_iterable_to_iter(only_caught_errors_stream, itype=itype))

    iterator = bi_iterable_to_iter(
        stream(map(throw, [TestError, ValueError]))
        .catch(ValueError, finally_raise=True)
        .catch(TestError, finally_raise=True),
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
            stream(map(throw, [ValueError, TypeError])).catch(
                Exception, when=adapt(lambda exception: "ValueError" in repr(exception))
            ),
            itype=itype,
        )
    # `catch` should be able to yield a non-None replacement
    assert to_list(
        stream(map(lambda n: 1 / n, [0, 1, 2, 4])).catch(
            ZeroDivisionError,
            replace=adapt(lambda e: float("inf")),
        ),
        itype=itype,
    ) == [float("inf"), 1, 0.5, 0.25]
    # `catch` should be able to yield a None replacement
    assert to_list(
        stream(map(lambda n: 1 / n, [0, 1, 2, 4])).catch(
            ZeroDivisionError,
            replace=adapt(lambda e: None),
        ),
        itype=itype,
    ) == [None, 1, 0.5, 0.25]

    errors_counter: Counter[Type[Exception]] = Counter()
    # `catch` should accept multiple types
    assert to_list(
        stream(
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
        ).catch(
            (ValueError, TestError, ZeroDivisionError),
            when=adapt(lambda err: errors_counter.update([type(err)]) is None),
        ),
        itype=itype,
    ) == list(map(lambda n: 1 / n, range(2, 10, 2)))
    # `catch` should accept multiple types
    assert errors_counter == {TestError: 5, ValueError: 3, ZeroDivisionError: 1}

    # `do` side effect should be correctly applied
    errors: List[Exception] = []
    to_list(
        stream([0, 1, 0, 1, 0])
        .map(lambda n: round(1 / n, 2))
        .catch(ZeroDivisionError, do=adapt(errors.append)),
        itype=itype,
    )
    assert len(errors) == 3


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe(itype: IterableType) -> None:
    def inverse(
        chars: str, every: Optional[Union[int, datetime.timedelta]] = None
    ) -> stream[float]:
        return (
            stream(chars)
            .map(int)
            .map(lambda n: 1 / n)
            .observe("inverses", every=every)
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
        #################
        # every == None #
        #################

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

        ##############
        # every == 2 #
        ##############

        # `observe` should reraise
        logs.clear()
        with pytest.raises(ZeroDivisionError):
            to_list(inverse("12---3456----07", every=2), itype=itype)
        # `observe` errors and yields independently
        assert logs == [
            Log(errors=0, yields=2),
            Log(errors=2, yields=2),
            Log(errors=3, yields=4),
            Log(errors=3, yields=6),
            Log(errors=4, yields=6),
            Log(errors=6, yields=6),
            Log(errors=8, yields=6),
        ]

        logs.clear()
        to_list(
            inverse("12---3456----07", every=2).catch(ZeroDivisionError), itype=itype
        )
        # `observe` should produce one last log on StopIteration
        assert logs == [
            Log(errors=0, yields=2),
            Log(errors=2, yields=2),
            Log(errors=3, yields=4),
            Log(errors=3, yields=6),
            Log(errors=4, yields=6),
            Log(errors=6, yields=6),
            Log(errors=8, yields=6),
            Log(errors=8, yields=7),
        ]

        logs.clear()
        to_list(
            inverse("12---3456----0", every=2).catch(ZeroDivisionError), itype=itype
        )
        # `observe` should skip redundant last log on StopIteration
        assert logs == [
            Log(errors=0, yields=2),
            Log(errors=2, yields=2),
            Log(errors=3, yields=4),
            Log(errors=3, yields=6),
            Log(errors=4, yields=6),
            Log(errors=6, yields=6),
            Log(errors=8, yields=6),
        ]

        ###############
        # every == 1s #
        ###############

        # `observe` should reraise
        logs.clear()
        with pytest.raises(ZeroDivisionError):
            to_list(
                inverse("12---3456----07", every=datetime.timedelta(seconds=1)),
                itype=itype,
            )
        # `observe` errors and yields independently
        assert logs == []

        logs.clear()
        to_list(
            inverse("12---3456----07", every=datetime.timedelta(seconds=1)).catch(
                ZeroDivisionError
            ),
            itype=itype,
        )
        # `observe` should produce one last log on StopIteration
        assert logs == [
            Log(errors=8, yields=7),
        ]

        logs.clear()
        to_list(
            inverse("12---3456----0", every=datetime.timedelta(seconds=1)).catch(
                ZeroDivisionError
            ),
            itype=itype,
        )
        # `observe` should skip redundant last log on StopIteration
        assert logs == [
            Log(errors=8, yields=6),
        ]


def test_is_iterable() -> None:
    assert isinstance(stream(ints_src), Iterable)
    assert isinstance(stream(ints_src), AsyncIterable)


def test_call() -> None:
    acc: List[int] = []
    ints = stream(ints_src).map(acc.append)
    # `__call__` should return the stream.
    assert ints() is ints
    # `__call__` should exhaust the stream.
    assert acc == list(ints_src)


@pytest.mark.asyncio
async def test_await() -> None:
    acc: List[int] = []
    ints = stream(ints_src).map(acc.append)
    # __await__ should return the stream.
    assert (await awaitable_to_coroutine(ints)) is ints
    # __await__ should exhaust the stream.
    assert acc == list(ints_src)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_multiple_iterations(itype: IterableType) -> None:
    ints = stream(ints_src)
    for _ in range(3):
        # The first iteration over a stream should yield the same elements as any subsequent iteration on the same stream, even if it is based on a `source` returning an iterator that only support 1 iteration.
        assert to_list(ints, itype=itype) == list(ints_src)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_pipe(itype: IterableType) -> None:
    def func(
        stream: stream, *ints: int, **strings: str
    ) -> Tuple[stream, Tuple[int, ...], Dict[str, str]]:
        return stream, ints, strings

    stream_ = stream(ints_src)
    ints = (0, 1, 2, 3)
    strings = {"foo": "bar", "bar": "foo"}
    # `pipe` should pass the stream and args/kwargs to `func`.
    assert stream_.pipe(func, *ints, **strings) == (stream_, ints, strings)
    # `pipe` should be ok without args and kwargs.
    assert stream_.pipe(to_list, itype=itype) == to_list(stream_, itype=itype)


def test_eq() -> None:
    threads = ThreadPoolExecutor(max_workers=10)
    big_stream = (
        stream(ints_src)
        .catch((TypeError, ValueError), replace=identity, when=identity)
        .catch((TypeError, ValueError), replace=async_identity, when=async_identity)
        .filter(identity)
        .filter(async_identity)
        .do(identity, concurrency=3)
        .do(async_identity, concurrency=3)
        .group(3, by=bool)
        .flatten(concurrency=3)
        .group(3, by=async_identity)
        .map(iter)
        .map(sync_to_async_iter)
        .flatten(concurrency=3)
        .groupby(bool)
        .groupby(async_identity)
        .map(identity, concurrency=threads)
        .map(async_identity)
        .observe("foo")
        .skip(3)
        .skip(3)
        .truncate(4)
        .truncate(4)
        .throttle(1, per=datetime.timedelta(seconds=1))
    )

    assert big_stream == (
        stream(ints_src)
        .catch((TypeError, ValueError), replace=identity, when=identity)
        .catch((TypeError, ValueError), replace=async_identity, when=async_identity)
        .filter(identity)
        .filter(async_identity)
        .do(identity, concurrency=3)
        .do(async_identity, concurrency=3)
        .group(3, by=bool)
        .flatten(concurrency=3)
        .group(3, by=async_identity)
        .map(iter)
        .map(sync_to_async_iter)
        .flatten(concurrency=3)
        .groupby(bool)
        .groupby(async_identity)
        .map(identity, concurrency=threads)
        .map(async_identity)
        .observe("foo")
        .skip(3)
        .skip(3)
        .truncate(4)
        .truncate(4)
        .throttle(1, per=datetime.timedelta(seconds=1))
    )
    assert big_stream != (
        stream(list(ints_src))  # not same source
        .catch((TypeError, ValueError), replace=lambda e: 2, when=identity)
        .catch(
            (TypeError, ValueError), replace=asyncify(lambda e: 2), when=async_identity
        )
        .filter(identity)
        .filter(async_identity)
        .do(identity, concurrency=3)
        .do(async_identity, concurrency=3)
        .group(3, by=bool)
        .flatten(concurrency=3)
        .group(3, by=async_identity)
        .map(iter)
        .map(sync_to_async_iter)
        .flatten(concurrency=3)
        .groupby(bool)
        .groupby(async_identity)
        .map(identity, concurrency=threads)
        .map(async_identity)
        .observe("foo")
        .skip(3)
        .skip(3)
        .truncate(4)
        .truncate(4)
        .throttle(1, per=datetime.timedelta(seconds=1))
    )
    assert big_stream != (
        stream(ints_src)
        .catch((TypeError, ValueError), replace=lambda e: 2, when=identity)
        .catch(
            (TypeError, ValueError), replace=asyncify(lambda e: 2), when=async_identity
        )
        .filter(identity)
        .filter(async_identity)
        .do(identity, concurrency=3)
        .do(async_identity, concurrency=3)
        .group(3, by=bool)
        .flatten(concurrency=3)
        .group(3, by=async_identity)
        .map(iter)
        .map(sync_to_async_iter)
        .flatten(concurrency=3)
        .groupby(bool)
        .groupby(async_identity)
        .map(identity, concurrency=threads)
        .map(async_identity)
        .observe("foo")
        .skip(3)
        .skip(3)
        .truncate(4)
        .truncate(4)
        .throttle(1, per=datetime.timedelta(seconds=2))  # not the same interval
    )


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_ref_cycles(itype: IterableType) -> None:
    async def async_int(o: Any) -> int:
        return int(o)

    ints = (
        stream("123_5")
        .map(async_int)
        .map(str)
        .group(1)
        .groupby(len)
        .catch(Exception, finally_raise=True)
    )
    exception: Exception
    try:
        to_list(ints, itype=itype)
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
    thread = threading.Thread(target=stream(iter(src.get, None)).do(zeros.append))
    thread.start()
    src.put("foo")
    src.put("bar")
    src.put(None)
    thread.join()
    # stream must work on Queue
    assert zeros == ["foo", "bar"]


def test_deepcopy() -> None:
    ints = stream([]).map(str)
    stream_copy = copy.deepcopy(ints)
    # the copy must be equal
    assert ints == stream_copy
    # the copy must be a different object
    assert ints is not stream_copy
    # the copy's source must be a different object
    assert ints.source is not stream_copy.source


def test_slots() -> None:
    ints = stream(ints_src).filter(bool)
    # a stream should not have a __dict__
    with pytest.raises(AttributeError):
        stream(ints_src).__dict__
    # a stream should have __slots__
    assert ints.__slots__ == ("_where",)
    # a stream should not have a __dict__
    with pytest.raises(AttributeError):
        ints.__dict__


def test_iter_loop_auto_closing() -> None:
    original_new_event_loop = asyncio.new_event_loop
    created_loop: "queue.Queue[asyncio.AbstractEventLoop]" = queue.Queue(maxsize=1)

    def tracking_new_event_loop():
        loop = original_new_event_loop()
        created_loop.put_nowait(loop)
        return loop

    asyncio.new_event_loop = tracking_new_event_loop
    iterator_a = iter(stream(ints_src).filter(async_identity))
    loop_a = created_loop.get_nowait()
    iterator_b = iter(stream(ints_src).filter(async_identity))
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
    "stream_",
    (
        stream(range(N)).map(slow_identity, concurrency=N // 8),
        (
            stream(range(N))
            .map(lambda i: map(slow_identity, (i,)))
            .flatten(concurrency=N // 8)
        ),
    ),
)
async def test_run_in_executor(stream_: stream) -> None:
    """
    Tests that executor-based concurrent mapping/flattening are wrapped
    in non-loop-blocking run_in_executor-based async tasks.
    """
    concurrency = N // 8
    res: tuple[int, int]

    async def count(stream_: stream) -> int:
        return len([_ async for _ in stream_])

    duration, res = await timecoro(
        lambda: asyncio.gather(count(stream_), count(stream_)), times=10
    )
    assert tuple(res) == (N, N)
    assert duration == pytest.approx(N * slow_identity_duration / concurrency, rel=0.25)
