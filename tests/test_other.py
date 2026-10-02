import asyncio
from concurrent.futures import ThreadPoolExecutor
import copy
from datetime import timedelta
import queue
import time
from typing import (
    Any,
    AsyncIterator,
    Callable,
    List,
    Type,
    Union,
)

import pytest

from streamable import stream
from tests.tools.error import TestBaseError
from tests.tools.func import (
    SLOW_IDENTITY_DURATION,
    async_throw_if_falsy_func,
    identity,
    noarg_asyncify,
    nothing,
    slow_identity,
    throw_func,
    throw_if_falsy_func,
)
from tests.tools.iter import (
    ITERABLE_TYPES,
    IterableType,
    acount,
    alist_or_list,
    aiter_or_iter,
    anext_or_next,
    stopiteration_type,
)
from tests.tools.source import INTEGERS, N, ints
from tests.tools.func import audit_async_func


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
        match=r"`source` must be Iterable or AsyncIterable or Callable but got: 1",
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
@pytest.mark.asyncio
async def test_aiter_of_concurrent_sync_operations(
    stream_factory: Callable[[], stream],
) -> None:
    """
    A stream involving sync concurrent mapping/flattening should not block the event loop.
    The event loop should be free to orchestrate the launch of concurrent map/flatten sync
    tasks (running in executors), among multiple stream iterations.
    """
    s1 = stream_factory()
    count_audit = await audit_async_func(lambda: acount(s1), times=3)

    async def parallel_counts(*streams: stream) -> List[int]:
        return list(await asyncio.gather(*(acount(s) for s in streams)))

    s2 = stream_factory()
    s3 = stream_factory()
    parallel_counts_audit = await audit_async_func(
        lambda: parallel_counts(s1, s2, s3), times=3
    )
    assert parallel_counts_audit.result == [
        count_audit.result,
        count_audit.result,
        count_audit.result,
    ]
    assert parallel_counts_audit.avg_duration == pytest.approx(
        count_audit.avg_duration, rel=0.2
    )


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


@pytest.mark.parametrize(
    "s",
    [
        ints,
        ints.catch(ValueError),
        ints.buffer(10),
        ints.do(str),
        ints.filter(lambda x: x % 2 == 0),
        ints.group(2).flatten(),
        ints.group(2).flatten(concurrency=2),
        ints.group(10),
        ints.group(10, by=lambda x: x % 2),
        ints.group(within=timedelta(seconds=1)),
        ints.map(str),
        ints.map(str, concurrency=2),
        ints.observe("ints", do=identity),
        ints.observe("ints", do=identity, every=N),
        ints.observe("ints", do=identity, every=timedelta(seconds=1)),
        ints.skip(10),
        ints.take(10),
        ints.throttle(N, per=timedelta(seconds=1)),
    ],
)
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_next_post_exhaustion(itype: IterableType, s: stream) -> None:
    """
    `__next__`/`__anext__` should raise `StopIteration`/`StopAsyncIteration`
    when called on an already exhausted iterator.
    """
    it = aiter_or_iter(s, itype)
    alist_or_list(it, itype)
    with pytest.raises((StopIteration, StopAsyncIteration)):
        anext_or_next(it, itype)


def test_stream_alias() -> None:
    from streamable import Stream

    assert stream is Stream


def test_loop_lifecycle() -> None:
    asyncio.get_event_loop_policy()._local._loop = None  # type: ignore

    def get_current_loop() -> asyncio.AbstractEventLoop:
        # return the loop set via set_event_loop
        return asyncio.get_event_loop_policy()._local._loop  # type: ignore

    # no loop set yet
    assert get_current_loop() is None
    s = stream([0, 0]).do(asyncio.sleep)
    # creating the stream does not set the loop
    assert get_current_loop() is None
    it = iter(s)
    # getting the iterator does not set the loop
    assert get_current_loop() is None
    assert next(it) == 0
    # the loop is set by the first next
    assert get_current_loop() is not None
    get_current_loop().close()
    with pytest.raises(RuntimeError, match="loop is closed"):
        next(it)
    it = iter(s)
    assert get_current_loop().is_closed()
    assert next(it) == 0
    # next on new iterator sets a new non-closed loop
    assert not get_current_loop().is_closed()
    assert list(it) == [0]
    # stopiteration does not close/unset the loop
    assert not get_current_loop().is_closed()


@pytest.mark.parametrize(
    "s, expected_yields, expected_error, iteration_resumes",
    [
        (
            stream([1, 2, 3, 0, 4]).map(throw_if_falsy_func(TestBaseError)),
            [1, 2, 3],
            TestBaseError,
            True,
        ),
        (
            stream([1, 2, 3, 0, 4])
            .map(throw_if_falsy_func(TestBaseError))
            .do(nothing, concurrency=2),
            [1],
            TestBaseError,
            False,
        ),
        # error in mapped func, FIFO
        (
            stream([1, 2, 3, 0, 4]).do(
                throw_if_falsy_func(TestBaseError), concurrency=2
            ),
            [1, 2, 3],
            TestBaseError,
            False,
        ),
        # error in mapped func, FDFO
        (
            stream([1, 2, 3, 0, 4])
            .do(lambda n: time.sleep(n / 10))
            .do(throw_if_falsy_func(TestBaseError), concurrency=2, as_completed=True),
            [1, 2, 3],
            TestBaseError,
            False,
        ),
        # asyncio.CancelledError in mapped func, FIFO
        (
            stream([1, 2, 3, 0, 4])
            .map(slow_identity)
            .do(
                async_throw_if_falsy_func(asyncio.CancelledError),
                concurrency=2,
            ),
            [1, 2, 3],
            asyncio.CancelledError,
            False,
        ),
        # asyncio.CancelledError in mapped func, FDFO
        (
            stream([1, 2, 3, 0, 4])
            .map(slow_identity)
            .do(
                async_throw_if_falsy_func(asyncio.CancelledError),
                concurrency=2,
                as_completed=True,
            ),
            [1, 2, 3],
            asyncio.CancelledError,
            False,
        ),
        (
            stream([1, 2, 3, 0, 4])
            .map(throw_if_falsy_func(TestBaseError))
            .group(1)
            .flatten(concurrency=2),
            [1, 2],
            TestBaseError,
            False,
        ),
        # BaseException thrown by inner iter
        (
            stream([1, 2, 3, 0, 4])
            .group(1)
            .map(lambda it: map(throw_if_falsy_func(TestBaseError), it))
            .flatten(concurrency=2),
            [1, 2, 3],
            TestBaseError,
            False,
        ),
        (
            stream([1, 2, 3, 0, 4])
            .map(throw_if_falsy_func(TestBaseError))
            .group(1, within=timedelta(seconds=1))
            .flatten(),
            [1, 2, 3],
            TestBaseError,
            False,
        ),
        (
            stream([1, 2, 3, 0, 4]).map(throw_if_falsy_func(TestBaseError)).buffer(1),
            [1, 2, 3],
            TestBaseError,
            False,
        ),
        # buffered elements are droped as soon as a base exception is received from upstream
        (
            stream([1, 2, 3, 0, 4]).map(throw_if_falsy_func(TestBaseError)).buffer(10),
            [],
            TestBaseError,
            False,
        ),
        (
            stream([1, 2, 3, 0, 4])
            .map(throw_if_falsy_func(TestBaseError))
            .catch(ValueError),
            [1, 2, 3],
            TestBaseError,
            True,
        ),
        (
            stream([1, 2, 3, 0, 4])
            .map(throw_if_falsy_func(TestBaseError))
            .group(1)
            .flatten(),
            [1, 2, 3],
            TestBaseError,
            True,
        ),
        (
            stream([1, 2, 3, 0, 4]).map(throw_if_falsy_func(TestBaseError)).skip(0),
            [1, 2, 3],
            TestBaseError,
            True,
        ),
        (
            stream([1, 2, 3, 0, 4]).map(throw_if_falsy_func(TestBaseError)).take(10),
            [1, 2, 3],
            TestBaseError,
            True,
        ),
        (
            stream([1, 2, 3, 0, 4])
            .map(throw_if_falsy_func(TestBaseError))
            .throttle(1, per=timedelta(microseconds=1)),
            [1, 2, 3],
            TestBaseError,
            True,
        ),
        (
            stream([1, 2, 3, 0, 4]).map(throw_if_falsy_func(TestBaseError)).observe(),
            [1, 2, 3],
            TestBaseError,
            True,
        ),
        (
            stream([1, 2, 3, 0, 4]).observe(
                every=1,
                do=lambda observation: throw_func(TestBaseError)(None)
                if observation.elements == 4
                else None,
            ),
            [1, 2, 3],
            TestBaseError,
            False,
        ),
        (
            stream([1, 2, 3, 0, 4])
            .map(slow_identity)
            .observe(
                every=timedelta(seconds=SLOW_IDENTITY_DURATION / 7),
                do=lambda observation: throw_func(TestBaseError)(None)
                if observation.elements == 3
                else None,
            ),
            [1, 2, 3],
            TestBaseError,
            False,
        ),
    ],
)
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_propagation_of_base_exceptions(
    itype: IterableType,
    s: stream[int],
    expected_yields: List[int],
    expected_error: Type[BaseException],
    iteration_resumes: bool,
):
    it = aiter_or_iter(s, itype)
    for expected_yield in expected_yields:
        assert anext_or_next(it, itype) == expected_yield
    with pytest.raises(expected_error):
        anext_or_next(it, itype)
    # after base exception, iteration is stopped if generators are involved, else it can resume
    if iteration_resumes:
        assert anext_or_next(it, itype) == 4
    else:
        with pytest.raises(stopiteration_type(itype)):
            anext_or_next(it, itype)
