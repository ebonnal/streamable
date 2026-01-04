import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import sys
import time
from pickle import PickleError
from typing import Any, Callable, Iterable, Iterator, List, Set, Type, cast

import pytest

from streamable import stream
from tests.utils.error import TestError
from tests.utils.functions import (
    async_identity,
    async_identity_sleep,
    async_randomly_slowed,
    async_slow_identity,
    async_square,
    async_throw_for_odd_func,
    async_throw_func,
    identity,
    identity_sleep,
    randomly_slowed,
    slow_identity,
    slow_identity_duration,
    square,
    throw_for_odd_func,
    throw_func,
)
from tests.utils.iteration import (
    ITERABLE_TYPES,
    IterableType,
    alist_or_list,
    anext_or_next,
    aiter_or_iter,
)
from tests.utils.source import N, EVEN_INTEGERS, INTEGERS
from tests.utils.timing import timestream


@pytest.mark.parametrize("concurrency", [1, 2])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map(concurrency: int, itype: IterableType) -> None:
    """At any concurrency the `map` method should act as the builtin map function, transforming elements while preserving input elements order."""
    assert alist_or_list(
        stream(INTEGERS).map(randomly_slowed(square), concurrency=concurrency),
        itype=itype,
    ) == list(map(square, INTEGERS))


@pytest.mark.parametrize("concurrency", [1, 100])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_async(concurrency: int, itype: IterableType) -> None:
    """At any concurrency the `map` method should act as the builtin map function with async transforms, preserving input order."""
    assert alist_or_list(
        stream(INTEGERS).map(
            async_randomly_slowed(async_square), concurrency=concurrency
        ),
        itype=itype,
    ) == list(map(square, INTEGERS))


@pytest.mark.parametrize("concurrency", [1, 2])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_do(concurrency: int, itype: IterableType) -> None:
    """At any concurrency the `do` method should return the upstream elements in order."""
    side_collection: Set[int] = set()

    def side_effect(x: int, func: Callable[[int], int]):
        nonlocal side_collection
        side_collection.add(func(x))

    res = alist_or_list(
        stream(INTEGERS).do(
            lambda i: randomly_slowed(side_effect(i, square)),
            concurrency=concurrency,
        ),
        itype=itype,
    )
    # At any concurrency the `do` method should return the upstream elements in order.
    assert res == list(INTEGERS)
    # At any concurrency the `do` method should call func on upstream elements (in any order).
    assert side_collection == set(map(square, INTEGERS))


@pytest.mark.parametrize("concurrency", [1, 100])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_do_async(concurrency: int, itype: IterableType) -> None:
    """At any concurrency the `do` method must preserve input elements order."""
    assert alist_or_list(
        stream(INTEGERS).do(
            async_randomly_slowed(async_square), concurrency=concurrency
        ),
        itype=itype,
    ) == list(INTEGERS)


@pytest.mark.parametrize(
    "operation, fn_name",
    [
        (stream.do, "effect"),
        (stream.map, "into"),
    ],
)
def test_executor_concurrency_with_async_function(
    operation: Callable[..., Any], fn_name: str
) -> None:
    """Executor concurrency should raise error when used with async functions."""
    with pytest.raises(
        TypeError,
        match=f"`concurrency` must be an int if `{fn_name}` is a coroutine function but got <concurrent.futures.thread.ThreadPoolExecutor object at .*",
    ):
        operation(
            stream(INTEGERS),
            async_identity_sleep,
            concurrency=ThreadPoolExecutor(max_workers=2),
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
def test_map_with_more_concurrency_than_elements(
    concurrency: int, n_elems: int, itype: IterableType
) -> None:
    """Map method should act correctly when concurrency > number of elements."""
    assert alist_or_list(
        stream(range(n_elems)).map(str, concurrency=concurrency), itype=itype
    ) == list(map(str, range(n_elems)))


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
def test_process_concurrency(
    ordered: bool,
    order_mutation: Callable[[Iterable], Iterable],
    itype: IterableType,
) -> None:
    """Process-based concurrency must correctly transform elements, respecting `ordered`."""

    def local_identity(x):
        return x  # pragma: no cover

    with ProcessPoolExecutor(10) as processes:
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
        assert alist_or_list(stream_, itype=itype) == expected_result_list
        # ... and should not mutate main thread-bound structures.
        assert state == [""] * len(sleeps)

        if sys.version_info >= (3, 9):
            for f in [lambda x: x, local_identity]:
                # process-based concurrency should not be able to serialize a lambda or a local func
                with pytest.raises(
                    (AttributeError, PickleError),
                    match="<locals>",
                ):
                    alist_or_list(
                        stream(INTEGERS).map(f, concurrency=processes), itype=itype
                    )
            # partial iteration
            assert (
                anext_or_next(aiter_or_iter(stream_, itype=itype), itype=itype)
                == expected_result_list[0]
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
    order_mutation: Callable[[Iterable], Iterable],
    expected_duration: float,
    operation: Callable[..., Any],
    func: Callable[..., Any],
    itype: IterableType,
) -> None:
    """Operation must respect `ordered` constraint."""
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
def test_map_or_do_concurrency(
    method: Callable[..., Any],
    func: Callable[..., Any],
    concurrency: int,
    itype: IterableType,
) -> None:
    """Increasing the concurrency of mapping should decrease proportionally the iteration's duration."""
    expected_iteration_duration = N * slow_identity_duration / concurrency
    duration, res = timestream(
        method(stream(INTEGERS), func, concurrency=concurrency), itype=itype
    )
    assert res == list(INTEGERS)
    # Increasing the concurrency of mapping should decrease proportionally the iteration's duration.
    assert duration == pytest.approx(expected_iteration_duration, rel=0.2)


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
def test_catched_error_upstream_of_map(
    itype: IterableType, concurrency: int, ordered: bool, expected: List[float]
) -> None:
    """At any concurrency, map/do should not stop iteration when upstream raises."""
    # at any concurrency, map/do should not stop iteration when upstream raises
    assert (
        alist_or_list(
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
        alist_or_list(
            stream([0, 1, 0, 2, 0])
            .map(lambda n: 1 / n)
            .map(async_identity_sleep, concurrency=concurrency, ordered=ordered)
            .catch(ZeroDivisionError, replace=lambda e: float("inf")),
            itype=itype,
        )
        == expected
    )


@pytest.mark.parametrize(
    "concurrency, method, throw_func, throw_for_odd_func, itype",
    [
        [
            concurrency,
            method,
            throw_func_,
            throw_for_odd_func_,
            itype,
        ]
        for concurrency in [1, 2]
        for method, throw_func_, throw_for_odd_func_ in [
            (stream.do, throw_func, throw_for_odd_func),
            (stream.do, async_throw_func, async_throw_for_odd_func),
            (stream.map, throw_func, throw_for_odd_func),
            (stream.map, async_throw_func, async_throw_for_odd_func),
        ]
        for itype in ITERABLE_TYPES
    ],
)
def test_map_or_do_with_exception(
    concurrency: int,
    method: Callable[[stream, Callable[[Any], int], int], stream],
    throw_func: Callable[[Type[Exception]], Callable[[Any], int]],
    throw_for_odd_func: Callable[[Type[Exception]], Callable[[Any], int]],
    itype: IterableType,
) -> None:
    """Map and do must handle exceptions correctly."""
    rasing_stream: stream[int] = method(
        stream(iter(INTEGERS)), throw_func(TestError), concurrency=concurrency
    )  # type: ignore
    # At any concurrency, `map` and `do` must raise.
    with pytest.raises(TestError):
        alist_or_list(rasing_stream, itype=itype)
    # Only `concurrency` upstream elements should be initially pulled for processing (0 if `concurrency=1`), and 1 more should be pulled for each call to `next`.
    assert next(cast(Iterator[int], rasing_stream.source)) == (
        concurrency + 1 if concurrency > 1 else concurrency
    )
    # At any concurrency, `map` and `do` should not stop after one exception occured.
    assert alist_or_list(
        method(
            stream(INTEGERS),
            throw_for_odd_func(TestError),
            concurrency=concurrency,  # type: ignore
        ).catch(TestError),
        itype=itype,
    ) == list(EVEN_INTEGERS)


@pytest.mark.parametrize("concurrency", [2, 4])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_partial_iteration_on_streams_using_concurrency(
    concurrency: int, itype: IterableType
) -> None:
    """Partial iteration should only pull necessary upstream elements."""
    yielded_elems = []

    def remembering_src() -> Iterator[int]:
        nonlocal yielded_elems
        for elem in INTEGERS:
            yielded_elems.append(elem)
            yield elem

    for stream_, n_pulls_after_first_next in [
        (
            stream(remembering_src()).map(identity, concurrency=concurrency),
            concurrency + 1,
        ),
        (
            stream(remembering_src()).map(async_identity, concurrency=concurrency),
            concurrency + 1,
        ),
        (
            stream(remembering_src()).do(identity, concurrency=concurrency),
            concurrency + 1,
        ),
        (
            stream(remembering_src()).do(async_identity, concurrency=concurrency),
            concurrency + 1,
        ),
        (
            stream(remembering_src()).group(1).flatten(concurrency=concurrency),
            concurrency,
        ),
    ]:
        yielded_elems = []
        iterator = aiter_or_iter(stream_, itype=itype)
        time.sleep(0.5)
        # before the first call to `next` a concurrent stream should have pulled 0 upstream elements.
        assert len(yielded_elems) == 0
        anext_or_next(iterator, itype=itype)
        time.sleep(0.5)
        # `after the first call to `next` a concurrent stream with given concurrency should have pulled only `n_pulls_after_first_next` upstream elements.
        assert len(yielded_elems) == n_pulls_after_first_next
