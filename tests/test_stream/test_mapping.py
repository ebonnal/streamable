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
@pytest.mark.parametrize(
    "randomly_slowed", [randomly_slowed(square), async_randomly_slowed(async_square)]
)
def test_map(
    concurrency: int, itype: IterableType, randomly_slowed: Callable[..., Any]
) -> None:
    """Map should transform elements correctly at any concurrency level."""
    assert alist_or_list(
        stream(INTEGERS).map(randomly_slowed, concurrency=concurrency),
        itype=itype,
    ) == list(map(square, INTEGERS))


@pytest.mark.parametrize("concurrency", [1, 2])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_do(concurrency: int, itype: IterableType) -> None:
    """Do should return upstream elements in order and call func on elements (order may vary with concurrency)."""
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
    assert res == list(INTEGERS)
    assert side_collection == set(map(square, INTEGERS))


@pytest.mark.parametrize("concurrency", [1, 100])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_do_async(concurrency: int, itype: IterableType) -> None:
    """Do should work with async functions at any concurrency level."""
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
    "concurrency, n_elems",
    [
        [16, 0],
        [1, 0],
        [16, 1],
        [16, 15],
        [16, 16],
    ],
)
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_with_more_concurrency_than_elements(
    concurrency: int, n_elems: int, itype: IterableType
) -> None:
    """Map method should act correctly when concurrency > number of elements."""
    assert alist_or_list(
        stream(range(n_elems)).map(str, concurrency=concurrency), itype=itype
    ) == list(map(str, range(n_elems)))


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
@pytest.mark.parametrize(
    "as_completed, order_mutation",
    [(False, identity), (True, sorted)],
)
def test_process_concurrency_ordering(
    as_completed: bool,
    order_mutation: Callable[[Iterable], Iterable],
    itype: IterableType,
) -> None:
    """Process-based concurrency should respect as_completed ordering."""
    with ProcessPoolExecutor(10) as processes:
        sleeps = [0.01, 1, 0.01]
        expected_result_list: List[str] = list(order_mutation(map(str, sleeps)))
        s = (
            stream(sleeps)
            .do(identity_sleep, concurrency=processes, as_completed=as_completed)
            .map(str, concurrency=processes, as_completed=False)
        )
        assert alist_or_list(s, itype=itype) == expected_result_list


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_process_concurrency_does_not_mutate_main_thread_state(
    itype: IterableType,
) -> None:
    """Process-based concurrency should not mutate main thread-bound structures."""
    with ProcessPoolExecutor(10) as processes:
        sleeps = [0.01, 1, 0.01]
        state: List[str] = []
        s = (
            stream(sleeps)
            .do(identity_sleep, concurrency=processes, as_completed=False)
            .map(str, concurrency=processes, as_completed=False)
            .do(state.append, concurrency=processes, as_completed=False)
            .do(lambda _: state.append(""), concurrency=1, as_completed=False)
        )
        alist_or_list(s, itype=itype)
        assert state == [""] * len(sleeps)


@pytest.mark.skipif(sys.version_info < (3, 9), reason="Requires Python 3.9+")
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_process_concurrency_raises_on_unserializable_functions(
    itype: IterableType,
) -> None:
    """Process-based concurrency should raise error for lambdas and local functions."""

    def local_identity(x):
        return x  # pragma: no cover

    with ProcessPoolExecutor(10) as processes:
        for f in [lambda x: x, local_identity]:
            with pytest.raises(
                (AttributeError, PickleError),
                match="<locals>",
            ):
                alist_or_list(
                    stream(INTEGERS).map(f, concurrency=processes), itype=itype
                )


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
@pytest.mark.parametrize(
    "as_completed, order_mutation",
    [(False, identity), (True, sorted)],
)
def test_process_concurrency_supports_partial_iteration(
    as_completed: bool,
    order_mutation: Callable[[Iterable], Iterable],
    itype: IterableType,
) -> None:
    """Process-based concurrency should support partial iteration."""
    with ProcessPoolExecutor(10) as processes:
        sleeps = [0.01, 1, 0.01]
        expected_result_list: List[str] = list(order_mutation(map(str, sleeps)))
        s = (
            stream(sleeps)
            .do(identity_sleep, concurrency=processes, as_completed=as_completed)
            .map(str, concurrency=processes, as_completed=False)
        )
        assert (
            anext_or_next(aiter_or_iter(s, itype=itype), itype=itype)
            == expected_result_list[0]
        )


@pytest.mark.parametrize(
    "as_completed, order_mutation, expected_duration",
    [
        (False, identity, 0.7),
        (True, sorted, 0.41),
    ],
)
@pytest.mark.parametrize(
    "operation, func",
    [
        (stream.do, time.sleep),
        (stream.map, identity_sleep),
        (stream.do, asyncio.sleep),
        (stream.map, async_identity_sleep),
    ],
)
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_mapping_ordering(
    as_completed: bool,
    order_mutation: Callable[[Iterable], Iterable],
    expected_duration: float,
    operation: Callable[..., Any],
    func: Callable[..., Any],
    itype: IterableType,
) -> None:
    """Operations should respect as_completed ordering, and unordering should improve runtime by avoiding bottlenecks."""
    seconds = [0.3, 0.01, 0.01, 0.4]
    duration, res = timestream(
        operation(stream(seconds), func, as_completed=as_completed, concurrency=2),
        5,
        itype=itype,
    )
    assert res == list(order_mutation(seconds))
    assert duration == pytest.approx(expected_duration, rel=0.2)


@pytest.mark.parametrize(
    "method, func",
    [
        (stream.do, slow_identity),
        (stream.do, async_slow_identity),
        (stream.map, slow_identity),
        (stream.map, async_slow_identity),
    ],
)
@pytest.mark.parametrize("concurrency", [1, 2, 4])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_or_do_concurrency(
    method: Callable[..., Any],
    func: Callable[..., Any],
    concurrency: int,
    itype: IterableType,
) -> None:
    """Increasing concurrency should decrease iteration duration proportionally."""
    expected_iteration_duration = N * slow_identity_duration / concurrency
    duration, res = timestream(
        method(stream(INTEGERS), func, concurrency=concurrency), itype=itype
    )
    assert res == list(INTEGERS)
    assert duration == pytest.approx(expected_iteration_duration, rel=0.2)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
@pytest.mark.parametrize(
    "as_completed, expected",
    (
        (False, [float("inf"), 1.0, float("inf"), 0.5, float("inf")]),
        (True, [float("inf"), float("inf"), float("inf"), 0.5, 1.0]),
    ),
)
@pytest.mark.parametrize("sleep", [identity_sleep, async_identity_sleep])
def test_map_handles_upstream_errors_with_concurrency(
    itype: IterableType,
    as_completed: bool,
    expected: List[float],
    sleep: Callable[..., Any],
) -> None:
    """Async map should handle upstream errors correctly with concurrency."""
    assert (
        alist_or_list(
            stream([0, 1, 0, 2, 0])
            .map(lambda n: 1 / n)
            .map(sleep, concurrency=2, as_completed=as_completed)
            .catch(ZeroDivisionError, replace=lambda e: float("inf")),
            itype=itype,
        )
        == expected
    )


@pytest.mark.parametrize("concurrency", [1, 2])
@pytest.mark.parametrize(
    "method, throw_func_",
    [
        (stream.do, throw_func),
        (stream.do, async_throw_func),
        (stream.map, throw_func),
        (stream.map, async_throw_func),
    ],
)
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_or_do_raises_on_exception(
    concurrency: int,
    method: Callable[[stream, Callable[[Any], int], int], stream],
    throw_func_: Callable[[Type[Exception]], Callable[[Any], int]],
    itype: IterableType,
) -> None:
    """Map and do should raise exceptions from the transform function."""
    raising_stream: stream[int] = method(
        stream(iter(INTEGERS)), throw_func_(TestError), concurrency=concurrency
    )  # type: ignore
    with pytest.raises(TestError):
        alist_or_list(raising_stream, itype=itype)


@pytest.mark.parametrize("concurrency", [1, 2])
@pytest.mark.parametrize(
    "method, throw_func_",
    [
        (stream.do, throw_func),
        (stream.do, async_throw_func),
        (stream.map, throw_func),
        (stream.map, async_throw_func),
    ],
)
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_or_do_pulls_source_elements_correctly(
    concurrency: int,
    method: Callable[[stream, Callable[[Any], int], int], stream],
    throw_func_: Callable[[Type[Exception]], Callable[[Any], int]],
    itype: IterableType,
) -> None:
    """Map and do should pull upstream elements based on concurrency level."""
    raising_stream: stream[int] = method(
        stream(iter(INTEGERS)), throw_func_(TestError), concurrency=concurrency
    )  # type: ignore
    with pytest.raises(TestError):
        alist_or_list(raising_stream, itype=itype)
    assert next(cast(Iterator[int], raising_stream.source)) == (
        concurrency + 1 if concurrency > 1 else concurrency
    )


@pytest.mark.parametrize("concurrency", [1, 2])
@pytest.mark.parametrize(
    "method, throw_for_odd_func_",
    [
        (stream.do, throw_for_odd_func),
        (stream.do, async_throw_for_odd_func),
        (stream.map, throw_for_odd_func),
        (stream.map, async_throw_for_odd_func),
    ],
)
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_or_do_handles_partial_exceptions(
    concurrency: int,
    method: Callable[[stream, Callable[[Any], int], int], stream],
    throw_for_odd_func_: Callable[[Type[Exception]], Callable[[Any], int]],
    itype: IterableType,
) -> None:
    """Map and do should continue processing after exceptions occur."""
    assert alist_or_list(
        method(
            stream(INTEGERS),
            throw_for_odd_func_(TestError),
            concurrency=concurrency,  # type: ignore
        ).catch(TestError),
        itype=itype,
    ) == list(EVEN_INTEGERS)


@pytest.mark.parametrize("concurrency", [2, 4])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_partial_iteration_sync_map_with_concurrency(
    concurrency: int, itype: IterableType
) -> None:
    """Partial iteration on sync map with concurrency should pull elements correctly."""
    yielded_elems = []

    def remembering_src() -> Iterator[int]:
        nonlocal yielded_elems
        for elem in INTEGERS:
            yielded_elems.append(elem)
            yield elem

    s = stream(remembering_src()).map(identity, concurrency=concurrency)
    iterator = aiter_or_iter(s, itype=itype)
    time.sleep(0.5)
    assert len(yielded_elems) == 0
    anext_or_next(iterator, itype=itype)
    time.sleep(0.5)
    assert len(yielded_elems) == concurrency + 1


@pytest.mark.parametrize("concurrency", [2, 4])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_partial_iteration_async_map_with_concurrency(
    concurrency: int, itype: IterableType
) -> None:
    """Partial iteration on async map with concurrency should pull elements correctly."""
    yielded_elems = []

    def remembering_src() -> Iterator[int]:
        nonlocal yielded_elems
        for elem in INTEGERS:
            yielded_elems.append(elem)
            yield elem

    s = stream(remembering_src()).map(async_identity, concurrency=concurrency)
    iterator = aiter_or_iter(s, itype=itype)
    time.sleep(0.5)
    assert len(yielded_elems) == 0
    anext_or_next(iterator, itype=itype)
    time.sleep(0.5)
    assert len(yielded_elems) == concurrency + 1


@pytest.mark.parametrize("concurrency", [2, 4])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_partial_iteration_sync_do_with_concurrency(
    concurrency: int, itype: IterableType
) -> None:
    """Partial iteration on sync do with concurrency should pull elements correctly."""
    yielded_elems = []

    def remembering_src() -> Iterator[int]:
        nonlocal yielded_elems
        for elem in INTEGERS:
            yielded_elems.append(elem)
            yield elem

    s = stream(remembering_src()).do(identity, concurrency=concurrency)
    iterator = aiter_or_iter(s, itype=itype)
    time.sleep(0.5)
    assert len(yielded_elems) == 0
    anext_or_next(iterator, itype=itype)
    time.sleep(0.5)
    assert len(yielded_elems) == concurrency + 1


@pytest.mark.parametrize("concurrency", [2, 4])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_partial_iteration_async_do_with_concurrency(
    concurrency: int, itype: IterableType
) -> None:
    """Partial iteration on async do with concurrency should pull elements correctly."""
    yielded_elems = []

    def remembering_src() -> Iterator[int]:
        nonlocal yielded_elems
        for elem in INTEGERS:
            yielded_elems.append(elem)
            yield elem

    s = stream(remembering_src()).do(async_identity, concurrency=concurrency)
    iterator = aiter_or_iter(s, itype=itype)
    time.sleep(0.5)
    assert len(yielded_elems) == 0
    anext_or_next(iterator, itype=itype)
    time.sleep(0.5)
    assert len(yielded_elems) == concurrency + 1


@pytest.mark.parametrize("concurrency", [2, 4])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_partial_iteration_flatten_with_concurrency(
    concurrency: int, itype: IterableType
) -> None:
    """Partial iteration on flatten with concurrency should pull elements correctly."""
    yielded_elems = []

    def remembering_src() -> Iterator[int]:
        nonlocal yielded_elems
        for elem in INTEGERS:
            yielded_elems.append(elem)
            yield elem

    s = stream(remembering_src()).group(1).flatten(concurrency=concurrency)
    iterator = aiter_or_iter(s, itype=itype)
    time.sleep(0.5)
    assert len(yielded_elems) == 0
    anext_or_next(iterator, itype=itype)
    time.sleep(0.5)
    assert len(yielded_elems) == concurrency
