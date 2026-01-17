import asyncio
from concurrent.futures import ThreadPoolExecutor
import copy
import gc
import time
from streamable._tools._func import _Syncified
import queue
from typing import (
    Any,
    AsyncIterator,
    Callable,
    List,
    Union,
    cast,
)

import pytest

from streamable import stream
from tests.utils.func import (
    async_identity,
    identity,
    noarg_asyncify,
    slow_identity,
)
from tests.utils.iter import (
    ITERABLE_TYPES,
    IterableType,
    acount,
    alist_or_list,
    aiter_or_iter,
    anext_or_next,
)
from tests.utils.ref import get_referees
from tests.utils.source import INTEGERS, N, ints
from tests.utils.timing import time_coroutine


def test_init() -> None:
    assert ints._source is INTEGERS
    assert ints.upstream is None
    assert ints.observe().source is INTEGERS


def test_attributes_immutability() -> None:
    with pytest.raises(AttributeError):
        ints.source = INTEGERS  # type: ignore
    with pytest.raises(AttributeError):
        ints.upstream = stream(INTEGERS)  # type: ignore


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_iter_source(itype: IterableType) -> None:
    it = aiter_or_iter(ints, itype)
    assert alist_or_list(stream(it), itype) == list(INTEGERS)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_aiter_source(itype: IterableType) -> None:
    elements = list(range(10))

    async def aiterator() -> AsyncIterator[int]:
        for i in elements:
            yield i

    assert alist_or_list(stream(aiterator()), itype) == elements


def test_source_function() -> None:
    it = ints.__iter__()

    def src() -> int:
        return next(it)

    assert list(stream(src)) == list(INTEGERS)


@pytest.mark.asyncio
async def test_source_async_function() -> None:
    it = ints.__aiter__()

    async def src() -> int:
        return await it.__anext__()

    assert [i async for i in stream(src)] == list(INTEGERS)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_source_type_error(itype: IterableType) -> None:
    with pytest.raises(
        TypeError,
        match=r"`source` must be Iterable or AsyncIterable or Callable but got 1",
    ):
        aiter_or_iter(stream(1), itype)  # type: ignore


@pytest.mark.parametrize("adapt", [identity, noarg_asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_queue_source(
    itype: IterableType,
    adapt: Callable[[Any], Any],
) -> None:
    q: queue.Queue[int] = queue.Queue()

    for i in range(10):
        q.put(i)

    s: stream[int] = stream(adapt(lambda: q.get(timeout=0.2))).catch(
        queue.Empty, stop=True
    )
    assert alist_or_list(s, itype) == list(range(10))


@pytest.mark.asyncio
async def test_queue_source_async() -> None:
    q: asyncio.Queue[int] = asyncio.Queue()

    for i in range(10):
        await q.put(i)

    async def aget() -> int:
        return await asyncio.wait_for(q.get(), timeout=0.2)

    s = stream(aget).catch(asyncio.TimeoutError, stop=True)
    assert [i async for i in s] == list(range(10))


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_add(itype: IterableType) -> None:
    s1 = stream(range(10))
    s2 = stream(range(10, 20))
    s3 = stream(range(20, 30))
    assert alist_or_list(s1 + s2 + s3, itype) == list(range(30))
    assert s1 + s2 + s3 == s1 + s2 + s3

    union_stream: stream[Union[int, str]] = ints + ints.map(str)
    assert alist_or_list(union_stream, itype) == list(INTEGERS) + list(
        map(str, INTEGERS)
    )


def test_call() -> None:
    store: List[int] = []
    pipeline = ints.do(store.append)
    assert pipeline() is pipeline
    assert store == list(INTEGERS)


@pytest.mark.asyncio
async def test_await() -> None:
    store: List[int] = []
    pipeline = ints.map(store.append)
    assert (await pipeline) is pipeline
    assert store == list(INTEGERS)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_multiple_iterations(itype: IterableType) -> None:
    for _ in range(2):
        assert alist_or_list(ints, itype) == list(INTEGERS)


def test_pipe() -> None:
    s = ints.pipe(stream.catch, ValueError, where=bool, replace=str)
    assert s == ints.catch(ValueError, where=bool, replace=str)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
# TODO: disable
@pytest.mark.parametrize("disable_gc", [False])
@pytest.mark.parametrize(
    "operation",
    [
        lambda s: s,
        lambda s: s.buffer(10),
        lambda s: s.map(str),
    ],
)
def test_ref_cycles(
    itype: IterableType, disable_gc: bool, operation: Callable[[stream], Any]
) -> None:
    if disable_gc:
        gc.disable()
    src = map(int, "12-34")
    it = aiter_or_iter(operation(stream(src)), itype)
    while True:
        try:
            anext_or_next(it, itype)
        except (StopIteration, StopAsyncIteration):
            break
        except Exception as e:
            error_id = id(e)
    if not disable_gc:
        gc.collect()
    time.sleep(0.5)
    assert error_id not in map(id, gc.get_objects())
    if disable_gc:
        gc.enable()


def test_deepcopy() -> None:
    s = stream([]).map(str)
    copied_s = copy.deepcopy(s)
    assert s == copied_s
    assert s is not copied_s
    assert s.source is not copied_s.source
    assert s.upstream is not copied_s.upstream


def test_copy() -> None:
    s = stream([]).map(str)
    copied_s = copy.copy(s)
    assert s == copied_s
    assert s is not copied_s
    assert s.source is copied_s.source
    assert s.upstream is copied_s.upstream


def test_slots() -> None:
    with pytest.raises(AttributeError):
        ints.__dict__


def test_iter_loop_auto_closing() -> None:
    """
    The loop attached to the sync iterators involving async functions should:
    - be shared among operations in the lineage
    - be closed when the final iterator is garbage collected
    """
    parent = ints.filter(async_identity)
    child = parent.map(async_identity)

    child_it = iter(child)
    assert isinstance(child_it, map)

    parent_it = get_referees(child_it)[0][0]
    assert isinstance(parent_it, filter)

    child_loop = cast(_Syncified, get_referees(child_it)[1]).loop
    parent_loop = cast(_Syncified, get_referees(parent_it)[1]).loop

    # both iterators share the same loop
    assert child_loop is parent_loop
    # the loop is not closed yet
    assert not child_loop.is_closed()
    # iteration should not close the loop
    assert next(child_it) == 1
    assert list(child_it) == list(INTEGERS)[2:]
    assert not child_loop.is_closed()
    # the loop is closed when the final iterator is garbage collected
    del child_it
    assert child_loop.is_closed()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stream_factory",
    (
        lambda: ints.map(slow_identity, concurrency=N // 8),
        lambda: ints.map(
            slow_identity, concurrency=ThreadPoolExecutor(max_workers=N // 8)
        ),
        lambda: ints.map(lambda i: map(slow_identity, (i,))).flatten(
            concurrency=N // 8
        ),
    ),
)
async def test_aiter_of_concurrent_sync_operations(
    stream_factory: Callable[[], stream],
) -> None:
    """
    A stream involving sync concurrent mapping/flattening should not block the event loop.
    The event loop should be free to orchestrate the launch of concurrent map/flatten sync
    tasks (running in executors), among multiple stream iterations.
    """
    s1 = stream_factory()
    single_stream_duration, single_stream_res = await time_coroutine(
        lambda: acount(s1), times=3
    )

    async def parrallel_counts(*streams: stream) -> List[int]:
        return list(await asyncio.gather(*(acount(s) for s in streams)))

    s2 = stream_factory()
    s3 = stream_factory()
    multiple_streams_duration, multiple_streams_res = await time_coroutine(
        lambda: parrallel_counts(s1, s2, s3), times=3
    )
    assert multiple_streams_res == [
        single_stream_res,
        single_stream_res,
        single_stream_res,
    ]
    assert multiple_streams_duration == pytest.approx(single_stream_duration, rel=0.2)


def test_in() -> None:
    """stream behaves like a basic iterable for the `in` operator"""
    s = stream(map(str, ints))
    # finds 0
    assert "0" in s
    # finds 1
    assert "1" in s

    s = stream(map(str, ints))
    # finds 0
    assert "0" in s
    # doesn't find 0, exhausts the stream in the process
    assert "0" not in s
    # doesn't find 1 because the stream is exhausted
    assert "1" not in s

    # source that support multiple iteration:
    s = stream(ints.map(str))
    # finds 0
    assert "0" in s
    # finds 0 again on a fresh source
    assert "0" in s
    # finds 1 on a fresh source
    assert "1" in s
