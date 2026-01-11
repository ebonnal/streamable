import asyncio
import copy
import datetime
import queue
import threading
import traceback
from types import TracebackType
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)

import pytest

from streamable import stream
from streamable._tools._async import awaitable_to_coroutine
from streamable._tools._func import star
from streamable._tools._iter import async_iter
from tests.utils.functions import (
    async_identity,
    identity,
    slow_identity,
    slow_identity_duration,
)
from tests.utils.iteration import (
    ITERABLE_TYPES,
    IterableType,
    alist_or_list,
    aiter_or_iter,
)
from tests.utils.source import N, INTEGERS
from tests.utils.timing import timecoro


def test_init() -> None:
    """Init."""
    ints = stream(INTEGERS)
    assert ints._source is INTEGERS
    assert ints.upstream is None
    assert (
        stream(INTEGERS)
        .group(100)
        .flatten()
        .map(identity)
        .map(async_identity)
        .do(identity)
        .do(async_identity)
        .catch(Exception)
        .observe("foo")
        .throttle(1, per=datetime.timedelta(seconds=1))
        .source
    ) is INTEGERS
    with pytest.raises(AttributeError):
        stream(INTEGERS).source = INTEGERS  # type: ignore
    with pytest.raises(AttributeError):
        stream(INTEGERS).upstream = stream(INTEGERS)  # type: ignore


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_async_src(itype: IterableType) -> None:
    """a stream with an async source must be collectable as an Iterable or as AsyncIterable"""
    assert alist_or_list(stream(async_iter(iter(INTEGERS))), itype) == list(INTEGERS)
    assert alist_or_list(stream(async_iter(iter(INTEGERS)).__aiter__()), itype) == list(
        INTEGERS
    )


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_iter(itype: IterableType) -> None:
    """iter(stream) must return an Iterator."""
    assert isinstance(aiter_or_iter(stream(INTEGERS), itype=itype), itype)
    with pytest.raises(
        TypeError,
        match=r"`source` must be Iterable or AsyncIterable or Callable but got 1",
    ):
        aiter_or_iter(stream(1), itype=itype)  # type: ignore


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_queue_source(itype: IterableType) -> None:
    """Queue Source."""
    from queue import Queue, Empty

    ints_queue: Queue[int] = Queue()
    ints: stream[int]

    async def aget() -> int:
        return ints_queue.get(timeout=2)

    def fill():
        for i in range(10):
            ints_queue.put(i)

    fill()
    ints = stream(lambda: ints_queue.get(timeout=2)).catch(Empty, stop=True)
    assert alist_or_list(ints, itype) == list(range(10))
    fill()
    ints = stream(aget).catch(Empty, stop=True)
    assert alist_or_list(ints, itype) == list(range(10))


@pytest.mark.asyncio
async def test_aqueue_source() -> None:
    """Async queue source should work correctly with asyncio queues."""
    from asyncio import Queue, TimeoutError

    ints_queue: Queue[int] = Queue()
    for i in range(10):
        await ints_queue.put(i)

    async def queue_get() -> int:
        return await asyncio.wait_for(ints_queue.get(), timeout=2)

    ints: stream[int] = stream(queue_get).catch(TimeoutError, stop=True)
    assert [i async for i in ints] == list(range(10))


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_add(itype: IterableType) -> None:
    """Add."""
    from streamable._stream import FlattenStream

    ints = stream(INTEGERS)
    assert isinstance(ints + ints, FlattenStream)

    stream_a = stream(range(10))
    stream_b = stream(range(10, 20))
    stream_c = stream(range(20, 30))
    assert alist_or_list(stream_a + stream_b + stream_c, itype=itype) == list(range(30))

    s = stream(range(10))
    s += stream(range(10, 20))
    s += stream(range(20, 30))
    assert alist_or_list(s, itype=itype) == list(range(30))

    union_stream: stream[Union[int, str]] = ints + ints.map(str)
    assert alist_or_list(union_stream, itype=itype) == list(INTEGERS) + list(
        map(str, INTEGERS)
    )


def test_call() -> None:
    """Call."""
    acc: List[int] = []
    ints = stream(INTEGERS).map(acc.append)
    assert ints() is ints
    assert acc == list(INTEGERS)


@pytest.mark.asyncio
async def test_await() -> None:
    """Await should return the stream and exhaust it."""
    acc: List[int] = []
    ints = stream(INTEGERS).map(acc.append)
    assert (await awaitable_to_coroutine(ints)) is ints
    assert acc == list(INTEGERS)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_multiple_iterations(itype: IterableType) -> None:
    """Multiple Iterations."""
    ints = stream(INTEGERS)
    for _ in range(3):
        assert alist_or_list(ints, itype=itype) == list(INTEGERS)


def test_pipe() -> None:
    """Pipe."""

    def func(
        stream: stream, *ints: int, **strings: str
    ) -> Tuple[stream, Tuple[int, ...], Dict[str, str]]:
        return stream, ints, strings

    s = stream(INTEGERS)
    ints = (0, 1, 2, 3)
    strings = {"foo": "bar", "bar": "foo"}
    assert s.pipe(func, *ints, **strings) == (s, ints, strings)
    assert s == s.pipe(identity)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_ref_cycles(itype: IterableType) -> None:
    """Ref Cycles."""

    async def async_int(o: Any) -> int:
        return int(o)

    errors: List[Exception] = []
    ints = (
        stream("123_5")
        .map(async_int)
        .map(str)
        .group(1, by=len)
        .map(star(lambda _, group: group))
        .catch(Exception, do=errors.append)
    )
    alist_or_list(ints, itype=itype)
    exception = errors[0]
    assert [
        (var, val)
        for frame, _ in traceback.walk_tb(exception.__traceback__)
        if frame is not cast(TracebackType, exception.__traceback__).tb_frame
        for var, val in frame.f_locals.items()
        if isinstance(val, Exception)
        and frame is cast(TracebackType, val.__traceback__).tb_frame
    ] == []


def test_on_queue_in_thread() -> None:
    """On Queue In Thread."""
    zeros: List[str] = []
    src: "queue.Queue[Optional[str]]" = queue.Queue()
    thread = threading.Thread(target=stream(iter(src.get, None)).do(zeros.append))
    thread.start()
    src.put("foo")
    src.put("bar")
    src.put(None)
    thread.join()
    assert zeros == ["foo", "bar"]


def test_deepcopy() -> None:
    """Deepcopy."""
    ints = stream([]).map(str)
    stream_copy = copy.deepcopy(ints)
    assert ints == stream_copy
    assert ints is not stream_copy
    assert ints.source is not stream_copy.source


def test_slots() -> None:
    """Slots."""
    ints = stream(INTEGERS).filter(bool)
    with pytest.raises(AttributeError):
        stream(INTEGERS).__dict__
    assert ints.__slots__ == ("_where",)
    with pytest.raises(AttributeError):
        ints.__dict__


def test_iter_loop_auto_closing() -> None:
    """Iter Loop Auto Closing."""
    original_new_event_loop = asyncio.new_event_loop
    created_loop: "queue.Queue[asyncio.AbstractEventLoop]" = queue.Queue(maxsize=1)

    def tracking_new_event_loop():
        loop = original_new_event_loop()
        created_loop.put_nowait(loop)
        return loop

    asyncio.new_event_loop = tracking_new_event_loop
    iterator_a = iter(stream(INTEGERS).filter(async_identity))
    loop_a = created_loop.get_nowait()
    iterator_b = iter(stream(INTEGERS).filter(async_identity))
    loop_b = created_loop.get_nowait()
    assert not loop_a.is_closed()
    assert not loop_b.is_closed()
    del iterator_a
    assert loop_a.is_closed()
    assert not loop_b.is_closed()
    del iterator_b
    assert loop_b.is_closed()
    asyncio.new_event_loop = original_new_event_loop


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "s",
    (
        stream(range(N)).map(slow_identity, concurrency=N // 8),
        (
            stream(range(N))
            .map(lambda i: map(slow_identity, (i,)))
            .flatten(concurrency=N // 8)
        ),
    ),
)
async def test_run_in_executor(s: stream) -> None:
    """
    Tests that executor-based concurrent mapping/flattening are wrapped
    in non-loop-blocking run_in_executor-based async tasks.
    """
    concurrency = N // 8
    res: tuple[int, int]

    async def count(s: stream) -> int:
        return len([_ async for _ in s])

    duration, res = await timecoro(lambda: asyncio.gather(count(s), count(s)), times=10)
    assert tuple(res) == (N, N)
    assert duration == pytest.approx(N * slow_identity_duration / concurrency, rel=0.25)
