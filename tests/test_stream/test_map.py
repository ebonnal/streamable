from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
import sys
from pickle import PickleError
import time
from typing import Any, Callable, Iterable, List, Union

import pytest

from streamable import stream
from tests.utils.func import (
    async_identity,
    async_identity_sleep,
    async_inverse_sleep,
    async_randomly_slowed,
    async_square,
    identity,
    identity_sleep,
    inverse,
    inverse_sleep,
    randomly_slowed,
    square,
)
from tests.utils.iter import (
    ITERABLE_TYPES,
    IterableType,
    alist_or_list,
    anext_or_next,
    aiter_or_iter,
)
from tests.utils.source import N, INTEGERS, ints


def test_map_async_func_with_executor():
    with pytest.raises(
        TypeError,
        match="`concurrency` must be an int if `into` is a coroutine function but got",
    ):
        ints.map(async_identity, concurrency=ThreadPoolExecutor(2))


@pytest.mark.parametrize("concurrency", [1, 2, N * 2])
@pytest.mark.parametrize(
    "randomly_slowed_square",
    [randomly_slowed(square), async_randomly_slowed(async_square)],
)
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_preserves_order(
    concurrency: int, itype: IterableType, randomly_slowed_square: Callable[..., Any]
) -> None:
    s = ints.map(randomly_slowed_square, concurrency=concurrency)
    assert alist_or_list(s, itype) == list(map(square, INTEGERS))


@pytest.mark.parametrize("as_completed, order", [(False, identity), (True, sorted)])
@pytest.mark.parametrize(
    "identity_sleep,concurrency",
    [
        (identity_sleep, 2),
        (identity_sleep, ProcessPoolExecutor(2)),
        (async_identity_sleep, 2),
    ],
)
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_concurrency_ordering(
    as_completed: bool,
    concurrency: Union[int, Executor],
    identity_sleep: Callable[..., Any],
    order: Callable[[Iterable], Iterable],
    itype: IterableType,
) -> None:
    sleeps = [0.01, 1, 0.5]
    s = stream(sleeps).map(
        identity_sleep, concurrency=concurrency, as_completed=as_completed
    )
    assert alist_or_list(s, itype) == list(order(sleeps))


@pytest.mark.parametrize("as_completed", [False, True])
@pytest.mark.parametrize("concurrency", [1, 2, 3])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_process_concurrency_partial_iteration(
    as_completed: bool, concurrency: int, itype: IterableType
) -> None:
    """
    Process-based concurrency should exit properly even if the stream is not exhausted.
    Pending tasks should run until completion before exiting the executor context.
    """
    sleeps = list(range(1, 10))
    start = time.perf_counter()
    with ProcessPoolExecutor(max_workers=concurrency) as processes:
        s = stream(sleeps).do(
            time.sleep, concurrency=processes, as_completed=as_completed
        )
        it = aiter_or_iter(s, itype)
        # this will start the execution of `concurrency` sleeps (`sleeps[:concurrency]``)
        assert anext_or_next(it, itype) == sleeps[0]
        assert time.perf_counter() - start == pytest.approx(1, rel=0.1)
        # now that the first sleep is done, the last one can start (`sleeps[concurrency]`)
        # we exit the context manager only when all pending tasks are completed.
    assert time.perf_counter() - start == pytest.approx(
        sleeps[0] + sleeps[concurrency], rel=0.2
    )


@pytest.mark.skipif(sys.version_info < (3, 9), reason="Requires Python 3.9+")
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_process_concurrency_raises_on_unserializable_functions(
    itype: IterableType,
) -> None:
    def local_identity(x):
        return x  # pragma: no cover

    with ProcessPoolExecutor(max_workers=2) as processes:
        for f in [lambda x: x, local_identity]:
            with pytest.raises((AttributeError, PickleError), match="<locals>"):
                alist_or_list(ints.map(f, concurrency=processes), itype)


@pytest.mark.parametrize(
    "concurrent, as_completed, expected_results",
    (
        (False, False, [float("inf"), 1.0, float("inf"), 0.5, float("inf")]),
        (True, False, [float("inf"), 1.0, float("inf"), 0.5, float("inf")]),
        (True, True, [float("inf"), float("inf"), float("inf"), 0.5, 1.0]),
    ),
)
@pytest.mark.parametrize("identity_sleep", [identity_sleep, async_identity_sleep])
@pytest.mark.parametrize("inverse_sleep", [inverse_sleep, async_inverse_sleep])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_with_errors(
    itype: IterableType,
    concurrent: bool,
    as_completed: bool,
    expected_results: List[float],
    identity_sleep: Callable[..., Any],
    inverse_sleep: Callable[..., Any],
) -> None:
    """Map should be resilient to errors happening upstream or in the transformation function itself."""
    # upstream errors
    concurrency = 2 if concurrent else 1
    s = (
        stream([0, 1, 0, 2, 0])
        .map(inverse)
        .map(identity_sleep, concurrency=concurrency, as_completed=as_completed)
        .catch(ZeroDivisionError, replace=lambda e: float("inf"))
    )
    assert alist_or_list(s, itype) == expected_results

    # map errors
    if concurrent:
        # when the error is upstream, it virtually increases the concurrency by 1
        # adds 1 to the concurrency to get the same behavior as when the error is in the transform function
        concurrency += 1
    s = (
        stream([0, 1, 0, 2, 0])
        .map(inverse_sleep, concurrency=concurrency, as_completed=as_completed)
        .catch(ZeroDivisionError, replace=lambda e: float("inf"))
    )
    assert alist_or_list(s, itype) == expected_results


@pytest.mark.parametrize("concurrency, pulled_elements", [(1, 1), (2, 3)])
@pytest.mark.parametrize("identity", [identity, async_identity])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_concurrent_buffersize(
    itype: IterableType,
    concurrency: int,
    pulled_elements: int,
    identity: Callable[..., Any],
) -> None:
    """
    Non concurrent map only pulls 1 element at a time,
    Concurrent map pulls `concurrency + 1` elements.
    """
    src = iter(range(10))
    s = stream(src).map(identity, concurrency=concurrency)
    it = aiter_or_iter(s, itype)
    assert anext_or_next(it, itype) == 0
    assert next(src) == pulled_elements
