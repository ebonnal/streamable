import asyncio
from concurrent.futures import ThreadPoolExecutor
import copy
import datetime
from operator import itemgetter
import queue
import re
import threading
import traceback
from types import TracebackType
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    cast,
)

import pytest

from streamable import stream
from streamable._tools._async import awaitable_to_coroutine
from streamable._tools._func import asyncify, star
from streamable._tools._iter import async_iter
from tests.utils import (
    ITERABLE_TYPES,
    IterableType,
    N,
    async_identity,
    bi_iterable_to_iter,
    identity,
    slow_identity,
    slow_identity_duration,
    ints_src,
    timecoro,
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
    assert to_list(stream(async_iter(iter(ints_src))), itype) == list(ints_src)
    # a stream with an async source must be collectable as an Iterable or as AsyncIterable
    assert to_list(stream(async_iter(iter(ints_src)).__aiter__()), itype) == list(
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

    async def foo():
        pass  # pragma: no cover

    foo_stream: stream = stream([]).filter(star(foo))
    assert (
        re.sub("0x[a-z0-9]+", "0x", repr(foo_stream))
        == "stream([]).filter(star(<function test_repr.<locals>.foo at 0x>))"
    )
    assert str(foo_stream) == "stream([]).filter(star(foo))"
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
        match=r"`source` must be Iterable or AsyncIterable or Callable but got 1",
    ):
        bi_iterable_to_iter(stream(1), itype=itype)  # type: ignore


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_queue_source(itype) -> None:
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
    assert to_list(ints, itype) == list(range(10))
    fill()
    ints = stream(aget).catch(Empty, stop=True)
    assert to_list(ints, itype) == list(range(10))


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
@pytest.mark.asyncio
async def test_aqueue_source(itype) -> None:
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
    second_item = itemgetter(1)
    big_stream = (
        stream(ints_src)
        .catch((TypeError, ValueError), replace=identity, where=identity)
        .catch((TypeError, ValueError), replace=async_identity, where=async_identity)
        .filter(identity)
        .filter(async_identity)
        .do(identity, concurrency=3)
        .do(async_identity, concurrency=3)
        .group(3, by=bool)
        .map(second_item)
        .flatten(concurrency=3)
        .group(3, by=async_identity)
        .map(second_item)
        .map(iter)
        .map(async_iter)
        .flatten(concurrency=3)
        .map(identity, concurrency=threads)
        .map(async_identity)
        .observe("foo")
        .skip(3)
        .skip(3)
        .take(4)
        .take(4)
        .throttle(1, per=datetime.timedelta(seconds=1))
    )

    assert big_stream == (
        stream(ints_src)
        .catch((TypeError, ValueError), replace=identity, where=identity)
        .catch((TypeError, ValueError), replace=async_identity, where=async_identity)
        .filter(identity)
        .filter(async_identity)
        .do(identity, concurrency=3)
        .do(async_identity, concurrency=3)
        .group(3, by=bool)
        .map(second_item)
        .flatten(concurrency=3)
        .group(3, by=async_identity)
        .map(second_item)
        .map(iter)
        .map(async_iter)
        .flatten(concurrency=3)
        .map(identity, concurrency=threads)
        .map(async_identity)
        .observe("foo")
        .skip(3)
        .skip(3)
        .take(4)
        .take(4)
        .throttle(1, per=datetime.timedelta(seconds=1))
    )
    assert big_stream != (
        stream(list(ints_src))  # not same source
        .catch((TypeError, ValueError), replace=lambda e: 2, where=identity)
        .catch(
            (TypeError, ValueError), replace=asyncify(lambda e: 2), where=async_identity
        )
        .filter(identity)
        .filter(async_identity)
        .do(identity, concurrency=3)
        .do(async_identity, concurrency=3)
        .group(3, by=bool)
        .map(second_item)
        .flatten(concurrency=3)
        .group(3, by=async_identity)
        .map(second_item)
        .map(iter)
        .map(async_iter)
        .flatten(concurrency=3)
        .map(identity, concurrency=threads)
        .map(async_identity)
        .observe("foo")
        .skip(3)
        .skip(3)
        .take(4)
        .take(4)
        .throttle(1, per=datetime.timedelta(seconds=1))
    )
    assert big_stream != (
        stream(ints_src)
        .catch((TypeError, ValueError), replace=lambda e: 2, where=identity)
        .catch(
            (TypeError, ValueError), replace=asyncify(lambda e: 2), where=async_identity
        )
        .filter(identity)
        .filter(async_identity)
        .do(identity, concurrency=3)
        .do(async_identity, concurrency=3)
        .group(3, by=bool)
        .map(second_item)
        .flatten(concurrency=3)
        .group(3, by=async_identity)
        .map(second_item)
        .map(iter)
        .map(async_iter)
        .flatten(concurrency=3)
        .map(identity, concurrency=threads)
        .map(async_identity)
        .observe("foo")
        .skip(3)
        .skip(3)
        .take(4)
        .take(4)
        .throttle(1, per=datetime.timedelta(seconds=2))  # not the same interval
    )


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_ref_cycles(itype: IterableType) -> None:
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
    to_list(ints, itype=itype)
    exception = errors[0]
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
