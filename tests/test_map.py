import asyncio
import sys
import threading
import time
from concurrent.futures import Executor, Future, ProcessPoolExecutor, ThreadPoolExecutor
from pickle import PickleError
from typing import Any, Callable, Iterable, Iterator, List, TypeVar, Union

import pytest

from streamable import stream
from streamable._tools import _afuture, _future
from tests.tools.func import (
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
from tests.tools.iter import (
    ITERABLE_TYPES,
    IterableType,
    aiter_or_iter,
    alist_or_list,
    anext_or_next,
)
from tests.tools.loop import TEST_LOOP
from tests.tools.source import INTEGERS, N, ints

T = TypeVar("T")


def noop_buffer(s: stream[T], up_to: int) -> stream[T]:
    return s


@pytest.mark.parametrize("buffer", [noop_buffer, stream.buffer])
def test_map_async_func_with_executor(
    buffer: Callable[["stream[int]", int], "stream[int]"],
) -> None:
    with pytest.raises(
        TypeError,
        match="`concurrency` must be an int if `into` is a coroutine function but got",
    ):
        buffer(ints, 2).map(async_identity, concurrency=ThreadPoolExecutor(2))


@pytest.mark.parametrize("concurrency", [1, 2, N * 2])
@pytest.mark.parametrize(
    "randomly_slowed_square",
    [randomly_slowed(square), async_randomly_slowed(async_square)],
)
@pytest.mark.parametrize("buffer", [noop_buffer, stream.buffer])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_preserves_order(
    concurrency: int,
    itype: IterableType,
    randomly_slowed_square: Callable[..., Any],
    buffer: Callable[["stream[int]", int], "stream[int]"],
) -> None:
    s = buffer(ints, concurrency).map(randomly_slowed_square, concurrency=concurrency)
    assert alist_or_list(s, itype) == list(map(square, INTEGERS))


@pytest.mark.parametrize("as_completed, order", [(False, identity), (True, sorted)])
@pytest.mark.parametrize(
    "identity_sleep, concurrency",
    [
        (identity_sleep, 2),
        (identity_sleep, ProcessPoolExecutor(2)),
        (async_identity_sleep, 2),
    ],
)
@pytest.mark.parametrize("buffer", [noop_buffer, stream.buffer])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_concurrency_ordering(
    as_completed: bool,
    concurrency: Union[int, Executor],
    identity_sleep: Callable[..., Any],
    order: Callable[[Iterable], Iterable],
    itype: IterableType,
    buffer: Callable[["stream[float]", int], "stream[float]"],
) -> None:
    sleeps = [0.01, 1, 0.5]
    s = buffer(stream(sleeps), len(sleeps)).map(
        identity_sleep, concurrency=concurrency, as_completed=as_completed
    )
    assert alist_or_list(s, itype) == list(order(sleeps))


@pytest.mark.parametrize("as_completed", [False, True])
@pytest.mark.parametrize("concurrency", [1, 2, 3])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_process_concurrency_partial_iteration(
    as_completed: bool,
    concurrency: int,
    itype: IterableType,
) -> None:
    """
    Process-based concurrency should exit properly even if the stream is not exhausted.
    Pending tasks should run until completion before exiting the executor context.
    """
    sleeps = list(range(1, 10))
    with ProcessPoolExecutor(max_workers=concurrency) as processes:
        s = stream(sleeps).do(
            time.sleep, concurrency=processes, as_completed=as_completed
        )
        it = aiter_or_iter(s, itype)
        # this will start the execution of `concurrency` sleeps (`sleeps[:concurrency]``)
        next_start = time.perf_counter()
        assert anext_or_next(it, itype) == sleeps[0]
        assert time.perf_counter() - next_start == pytest.approx(sleeps[0], rel=0.2)
        termination_start = time.perf_counter()
        # now that the first sleep is done, the last one can start (`sleeps[concurrency]`)
        # we exit the context manager only when all pending tasks are completed.
    assert time.perf_counter() - termination_start == pytest.approx(
        sleeps[concurrency], rel=0.15
    )


@pytest.mark.skipif(sys.version_info < (3, 9), reason="Requires Python 3.9+")
@pytest.mark.parametrize("buffer", [noop_buffer, stream.buffer])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_process_concurrency_raises_on_unserializable_functions(
    itype: IterableType,
    buffer: Callable[["stream[int]", int], "stream[int]"],
) -> None:
    def local_identity(x):
        return x  # pragma: no cover

    with ProcessPoolExecutor(max_workers=2) as processes:
        for f in [lambda x: x, local_identity]:
            with pytest.raises((AttributeError, PickleError), match="<locals>"):
                alist_or_list(buffer(ints, 2).map(f, concurrency=processes), itype)


@pytest.mark.parametrize(
    "concurrency, as_completed, expected_results, buffer",
    (
        (1, False, [float("inf"), 1.0, float("inf"), 0.5, float("inf")], noop_buffer),
        (2, False, [float("inf"), 1.0, float("inf"), 0.5, float("inf")], noop_buffer),
        (2, True, [float("inf"), float("inf"), float("inf"), 0.5, 1.0], noop_buffer),
        (1, False, [float("inf"), 1.0, float("inf"), 0.5, float("inf")], stream.buffer),
        (3, False, [float("inf"), 1.0, float("inf"), 0.5, float("inf")], stream.buffer),
        (3, True, [float("inf"), float("inf"), float("inf"), 0.5, 1.0], stream.buffer),
    ),
)
@pytest.mark.parametrize("identity_sleep", [identity_sleep, async_identity_sleep])
@pytest.mark.parametrize("inverse_sleep", [inverse_sleep, async_inverse_sleep])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_with_errors(
    itype: IterableType,
    concurrency: int,
    as_completed: bool,
    expected_results: List[float],
    identity_sleep: Callable[..., Any],
    inverse_sleep: Callable[..., Any],
    buffer: Callable[["stream[float]", int], "stream[float]"],
) -> None:
    """Map should be resilient to errors happening upstream or in the transformation function itself."""
    # upstream errors
    s = (
        buffer(stream([0, 1, 0, 2, 0]).map(inverse), concurrency)
        .map(identity_sleep, concurrency=concurrency, as_completed=as_completed)
        .catch(ZeroDivisionError, replace=lambda e: float("inf"))
    )
    assert alist_or_list(s, itype) == expected_results

    # map errors
    if concurrency > 1:
        # when the error is upstream, it virtually increases the concurrency by 1
        # adds 1 to the concurrency to get the same behavior as when the error is in the transform function
        concurrency += 1
    s = (
        buffer(stream([0, 1, 0, 2, 0]), concurrency)
        .map(inverse_sleep, concurrency=concurrency, as_completed=as_completed)
        .catch(ZeroDivisionError, replace=lambda e: float("inf"))
    )
    assert alist_or_list(s, itype) == expected_results


@pytest.mark.parametrize(
    "concurrency, pulled_elements, buffer",
    [
        (1, 1, noop_buffer),
        (2, 3, noop_buffer),
        (1, 2, stream.buffer),
        (2, 4, stream.buffer),
    ],
)
@pytest.mark.parametrize("identity", [identity, async_identity])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_concurrent_buffersize(
    itype: IterableType,
    concurrency: int,
    pulled_elements: int,
    identity: Callable[..., Any],
    buffer: Callable[["stream[int]", int], "stream[int]"],
) -> None:
    """
    Non concurrent map only pulls 1 element at a time,
    Concurrent map pulls `concurrency + 1` elements.
    """
    src = iter(range(10))
    s = buffer(stream(src), concurrency).map(identity, concurrency=concurrency)
    it = aiter_or_iter(s, itype)
    assert anext_or_next(it, itype) == 0
    TEST_LOOP.run_until_complete(asyncio.sleep(0.1))
    assert next(src) == pulled_elements


def test_map_fdfo_late_arriving_done_future_is_dropped() -> None:
    future_results: _future.FDFOFutureResults[int] = _future.FDFOFutureResults()
    future: "Future[int]" = Future()
    future_results.add(future)
    future_results.clear()
    # a future gets done post-clear
    future.set_result(0)
    # that newly done future is dropped and not stored
    assert future_results._done_futures.empty()


@pytest.mark.asyncio
async def test_map_fdfo_late_arriving_done_future_is_dropped_async() -> None:
    future_results: _afuture.FDFOFutureResults[int] = _afuture.FDFOFutureResults()
    future: "asyncio.Future[int]" = asyncio.Future()
    future_results.add(future)
    future_results.clear()
    # a future gets done post-clear
    future.set_result(0)
    # that newly done future is dropped and not stored
    assert future_results._done_futures.empty()


@pytest.mark.parametrize(
    "identity, sleep, itype",
    [(identity, time.sleep, "sync"), (async_identity, asyncio.sleep, "async")],
)
@pytest.mark.parametrize("as_completed", [False, True])
def test_map_concurrent_with_upstream_buffer_not_blocked_by_slow_upstream(
    itype: IterableType,
    identity: Callable[..., Any],
    sleep: Callable[..., Any],
    as_completed: bool,
) -> None:
    s = (
        stream(range(10))
        .do(sleep)
        .buffer(3)
        .map(identity, concurrency=2, as_completed=as_completed)
    )
    start = time.perf_counter()
    it = aiter_or_iter(s, itype)
    assert anext_or_next(it, itype) == 0
    assert anext_or_next(it, itype) == 1
    assert time.perf_counter() - start == pytest.approx(1, rel=0.15)


@pytest.mark.parametrize("ending", ["empty", "late"])
@pytest.mark.parametrize("as_completed", [False, True])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_concurrent_with_upstream_buffer_terminates(
    itype: IterableType, as_completed: bool, ending: str
) -> None:
    def late_ending_source() -> Iterator[int]:
        yield 0
        # the upstream ends after the map has yielded its last result
        time.sleep(0.05)

    source = [] if ending == "empty" else late_ending_source()
    s = stream(source).buffer().map(identity, concurrency=2, as_completed=as_completed)
    results: List[List[int]] = []
    # iterate in a thread, so that a hang fails this test instead of blocking the suite
    thread = threading.Thread(
        target=lambda: results.append(alist_or_list(s, itype)), daemon=True
    )
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive(), "the iteration hangs"
    assert results == [[] if ending == "empty" else [0]]
