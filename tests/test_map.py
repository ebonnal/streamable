import asyncio
import queue
import sys
import time
from collections import Counter
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
from pickle import PickleError
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Iterable,
    List,
    Optional,
    Union,
)

import pytest

from streamable import _aiterators, _iterators, stream
from tests.tools.error import TestBaseError
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
    throw_func,
)
from tests.tools.iter import (
    ITERABLE_TYPES,
    IterableType,
    aclose_or_close,
    aidentity_or_identity,
    aiter_or_iter,
    alist_or_list,
    anext_or_next,
    aqueue_or_queue,
    stopiteration_type,
)
from tests.tools.loop import TEST_LOOP
from tests.tools.skip import mark_timing_sensitive
from tests.tools.source import INTEGERS, N, ints


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
@pytest.mark.parametrize(
    "apply_opt_buffer", [identity, lambda s: s.buffer(sys.maxsize)]
)
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_preserves_order(
    concurrency: int,
    itype: IterableType,
    randomly_slowed_square: Callable[..., Any],
    apply_opt_buffer: Callable[[stream[int]], stream[int]],
) -> None:
    s = apply_opt_buffer(ints).map(randomly_slowed_square, concurrency=concurrency)
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
@pytest.mark.parametrize(
    "apply_opt_buffer", [identity, lambda s: s.buffer(sys.maxsize)]
)
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_concurrency_ordering(
    as_completed: bool,
    concurrency: Union[int, Executor],
    identity_sleep: Callable[..., Any],
    order: Callable[[Iterable], Iterable],
    itype: IterableType,
    apply_opt_buffer: Callable[[stream[float]], stream[float]],
) -> None:
    sleeps = [0.01, 1, 0.5]
    s = apply_opt_buffer(stream(sleeps)).map(
        identity_sleep, concurrency=concurrency, as_completed=as_completed
    )
    assert alist_or_list(s, itype) == list(order(sleeps))


@mark_timing_sensitive
@pytest.mark.parametrize("as_completed", [False, True])
@pytest.mark.parametrize("concurrency", [1, 2, 3])
@pytest.mark.parametrize(
    "apply_opt_buffer", [identity, lambda s: s.buffer(sys.maxsize)]
)
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_process_concurrency_partial_iteration(
    as_completed: bool,
    concurrency: int,
    itype: IterableType,
    apply_opt_buffer: Callable[[stream[int]], stream[int]],
) -> None:
    """
    Process-based concurrency should exit properly even if the stream is not exhausted.
    Pending tasks should run until completion before exiting the executor context.
    """
    sleeps = list(range(1, 10))
    start = time.perf_counter()
    with ProcessPoolExecutor(max_workers=concurrency) as processes:
        s = apply_opt_buffer(stream(sleeps)).do(
            time.sleep, concurrency=processes, as_completed=as_completed
        )
        it = aiter_or_iter(s, itype)
        # this will start the execution of `concurrency` sleeps (`sleeps[:concurrency]``)
        assert anext_or_next(it, itype) == sleeps[0]
        assert time.perf_counter() - start == pytest.approx(1, rel=0.25)
        # now that the first sleep is done, the last one can start (`sleeps[concurrency]`)
        # we exit the context manager only when all pending tasks are completed.
    assert time.perf_counter() - start == pytest.approx(
        sleeps[0] + sleeps[concurrency], rel=0.2
    )


@pytest.mark.skipif(sys.version_info < (3, 9), reason="Requires Python 3.9+")
@pytest.mark.parametrize(
    "apply_opt_buffer", [identity, lambda s: s.buffer(sys.maxsize)]
)
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_process_concurrency_raises_on_unserializable_functions(
    itype: IterableType,
    apply_opt_buffer: Callable[[stream[int]], stream[int]],
) -> None:
    def local_identity(x):
        return x  # pragma: no cover

    with ProcessPoolExecutor(max_workers=2) as processes:
        for f in [lambda x: x, local_identity]:
            with pytest.raises((AttributeError, PickleError), match="<locals>"):
                alist_or_list(
                    apply_opt_buffer(ints).map(f, concurrency=processes), itype
                )


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
@pytest.mark.parametrize(
    "apply_opt_buffer", [identity, lambda s: s.buffer(sys.maxsize)]
)
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_with_errors(
    itype: IterableType,
    concurrent: bool,
    as_completed: bool,
    expected_results: List[float],
    identity_sleep: Callable[..., Any],
    inverse_sleep: Callable[..., Any],
    apply_opt_buffer: Callable[[stream[float]], stream[float]],
) -> None:
    """Map should be resilient to errors happening upstream or in the transformation function itself."""
    # upstream errors
    concurrency = 2 if concurrent else 1
    s = (
        apply_opt_buffer(stream([0, 1, 0, 2, 0]).map(inverse))
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
        apply_opt_buffer(stream([0, 1, 0, 2, 0]))
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


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
@pytest.mark.parametrize("as_completed", [False, True])
def test_map_concurrent_with_upstream_buffer_when_upstream_starves(
    itype: IterableType, as_completed: bool
) -> None:
    qu: Union[asyncio.Queue[Optional[int]], "queue.Queue[Optional[int]]"] = (
        aqueue_or_queue(itype)
    )
    qu.put_nowait(0)
    src = qu.get if isinstance(qu, asyncio.Queue) else iter(qu.get, None)
    s = (
        stream(src)
        .buffer(2)
        .map(aidentity_or_identity(itype), concurrency=2, as_completed=as_completed)
    )
    it = aiter_or_iter(s, itype)
    assert anext_or_next(it, itype) == 0
    qu.put_nowait(1)
    qu.put_nowait(2)
    assert {anext_or_next(it, itype), anext_or_next(it, itype)} == {1, 2}
    if isinstance(it, AsyncIterator):
        with pytest.raises(asyncio.TimeoutError):

            async def timeout_next() -> None:
                await asyncio.wait_for(it.__anext__(), timeout=0.1)

            TEST_LOOP.run_until_complete(timeout_next())
    qu.put_nowait(3)
    assert anext_or_next(it, itype) == 3
    qu.put_nowait(None)
    aclose_or_close(it, itype)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
@pytest.mark.parametrize("as_completed, collect", [(False, list), (True, Counter)])
def test_map_concurrent_with_upstream_buffer_exhaustion(
    itype: IterableType,
    as_completed: bool,
    collect: Callable[..., Any],
) -> None:
    src = range(10)
    s = (
        stream(src)
        .buffer(2)
        .map(aidentity_or_identity(itype), concurrency=2, as_completed=as_completed)
    )
    it = aiter_or_iter(s, itype)
    assert collect(alist_or_list(it, itype)) == collect(src)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_map_concurrent_with_upstream_buffer_after_interrupted_exhaustion(
    itype: IterableType, monkeypatch: pytest.MonkeyPatch
) -> None:
    puller_type = _aiterators._Puller if itype == "async" else _iterators._Puller
    # interrupt the `._terminate` on exhaustion, before the STOP_ITERATION is queued
    s = stream(range(4)).buffer(2).map(aidentity_or_identity(itype), concurrency=2)
    it = aiter_or_iter(s, itype)
    monkeypatch.setattr(puller_type, "wait", throw_func(TestBaseError))
    assert anext_or_next(it, itype) == 0
    assert anext_or_next(it, itype) == 1
    with pytest.raises(TestBaseError):
        anext_or_next(it, itype)
    assert anext_or_next(it, itype) == 2
    assert anext_or_next(it, itype) == 3
    with pytest.raises(stopiteration_type(itype)):
        anext_or_next(it, itype)
