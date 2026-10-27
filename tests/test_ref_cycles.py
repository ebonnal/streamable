import asyncio
import gc
import time
import weakref
from datetime import timedelta
from typing import (
    Any,
    Callable,
    Iterator,
    List,
    Type,
    TypeVar,
    cast,
)

import pytest

from streamable import stream
from streamable._tools._iter import ClosableAsyncIterator
from tests.tools.error import TestBaseError, TestError
from tests.tools.func import (
    SLOW_IDENTITY_DURATION,
    async_slow_identity,
    identity,
    slow_identity,
    throw_if_falsy_func,
)
from tests.tools.gc import disabled_gc, find_object
from tests.tools.iter import (
    ITERABLE_TYPES,
    IterableType,
    aiter_or_iter,
    alist_or_list,
    anext_or_next,
)
from tests.tools.loop import TEST_LOOP

T = TypeVar("T")


operate_cases: List[Callable[[Iterator[int], Type[BaseException]], Any]] = [
    lambda raising_src, _: stream(raising_src).buffer(10),
    lambda raising_src, _: stream(raising_src).catch(ValueError),
    lambda raising_src, _: stream(raising_src).do(identity),
    lambda raising_src, _: stream(raising_src).filter(),
    lambda raising_src, _: stream(raising_src).group(2).flatten(),
    lambda raising_src, _: stream(raising_src).group(2).flatten(concurrency=2),
    lambda raising_src, _: stream(raising_src).group(2).flatten(concurrency=10),
    lambda raising_src, _: stream(raising_src).group(1, by=identity).flatten(),
    lambda raising_src, _: stream(raising_src).group(
        1, by=identity, within=timedelta(seconds=1)
    ),
    lambda raising_src, _: stream(raising_src).map(identity),
    lambda raising_src, _: stream(raising_src).map(identity, concurrency=2),
    lambda raising_src, _: stream(raising_src).map(identity, concurrency=10),
    lambda raising_src, _: stream(raising_src).map(
        identity, concurrency=2, as_completed=True
    ),
    lambda raising_src, _: stream(raising_src).map(
        identity, concurrency=10, as_completed=True
    ),
    lambda raising_src, _: stream(raising_src).observe(),
    lambda raising_src, _: stream(raising_src).observe(every=1000),
    lambda raising_src, _: stream(raising_src).observe(every=timedelta(seconds=1)),
    lambda raising_src, _: stream(raising_src).skip(10),
    lambda raising_src, _: stream(raising_src).take(10),
    lambda raising_src, _: stream(raising_src).throttle(10, per=timedelta(seconds=1)),
    # errors raised by the mapped function / a flattened inner iterator
    lambda _, error_type: stream([1, 2, 3, 0]).map(throw_if_falsy_func(error_type)),
    lambda _, error_type: stream([1, 2, 3, 0]).map(
        throw_if_falsy_func(error_type), concurrency=2
    ),
    lambda _, error_type: stream([1, 2, 3, 0]).map(
        throw_if_falsy_func(error_type), concurrency=2, as_completed=True
    ),
    lambda _, error_type: stream([1, 2, 3, 0])
    .group(2)
    .map(lambda g: map(throw_if_falsy_func(error_type), g))
    .flatten(),
    lambda _, error_type: stream([1, 2, 3, 0])
    .group(2)
    .map(lambda g: map(throw_if_falsy_func(error_type), g))
    .flatten(concurrency=2),
    lambda _, error_type: stream([1, 2, 3, 0])
    .group(2)
    .map(lambda g: stream(g).map(throw_if_falsy_func(error_type)))
    .flatten(),
    lambda _, error_type: stream([1, 2, 3, 0])
    .group(2)
    .map(lambda g: stream(g).map(throw_if_falsy_func(error_type)))
    .flatten(concurrency=2),
]


@pytest.mark.parametrize("operate", operate_cases)
@pytest.mark.parametrize("error_type", (TestError, TestBaseError))
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
@pytest.mark.asyncio
async def test_ref_cycles(
    itype: IterableType,
    operate: Callable[[Iterator[int], Type[BaseException]], Any],
    error_type: Type[BaseException],
) -> None:
    with disabled_gc():
        raising_src = map(throw_if_falsy_func(error_type), [1, 2, 3, 0])
        it = aiter_or_iter(operate(raising_src, error_type), itype)
        # complete iteration, capturing the error's id
        while True:
            try:
                if isinstance(it, Iterator):
                    it.__next__()
                else:
                    await it.__anext__()
            except error_type as e:
                error_id = id(e)
                break

        # let the event loop run the callbacks still referencing the futures
        await asyncio.sleep(0.01)

        # At this point the error should have been garbage collected (ref count)

        # Get the object if it still exists
        error = find_object(error_id, error_type)

        # objgraph.show_backrefs([error], filename="cycles.png", max_depth=10)

        assert error is None


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_ref_cycles_with_exhausted_flattened_iterators(
    itype: IterableType,
) -> None:
    with disabled_gc():
        iterators = [(n for n in [0]), (n for n in [1])]
        refs = [weakref.ref(iterator) for iterator in iterators]
        assert alist_or_list(stream(iterators).flatten(concurrency=2), itype) == [0, 1]
        del iterators
        # the exhausted iterators have been garbage collected (ref count)
        assert not any(ref() for ref in refs)


@pytest.mark.parametrize("as_completed", [False, True])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_ref_cycles_with_in_flight_errors(
    itype: IterableType, as_completed: bool, caplog: pytest.LogCaptureFixture
) -> None:
    with disabled_gc():
        s = (
            stream("1---")
            # sleep to ensure FDFO follows FIFO
            .map(slow_identity)
            .map(int, concurrency=4, as_completed=as_completed)
        )
        it = aiter_or_iter(s, itype)
        assert anext_or_next(it, itype) == 1
        # wait for the 3 errors to happen and be in-flight
        TEST_LOOP.run_until_complete(asyncio.sleep(SLOW_IDENTITY_DURATION * 5))
        if itype == "async":
            TEST_LOOP.run_until_complete(cast(ClosableAsyncIterator[int], it).aclose())
        del it
        # the in-flight errors have been garbage collected (ref count)
        assert not any(isinstance(obj, ValueError) for obj in gc.get_objects())
    # no "Future exception was never retrieved"
    assert not caplog.records


@pytest.mark.parametrize(
    "operate",
    [
        # the error gets pulled and queued while `1` is being consumed
        lambda s: s.buffer(1),
        # `[1]` is yielded on `within` timeout while the error's pull is in flight
        lambda s: s.group(5, within=timedelta(seconds=SLOW_IDENTITY_DURATION / 10)),
    ],
)
@pytest.mark.parametrize(
    "itype, slow_identity",
    [("sync", slow_identity), ("async", async_slow_identity)],
)
def test_ref_cycles_with_errors_left_queued_by_pullers(
    itype: IterableType,
    operate: Callable[[stream], stream],
    slow_identity: Callable[[Any], Any],
) -> None:
    with disabled_gc():
        s = operate(stream("1-").map(slow_identity).map(int))
        it = aiter_or_iter(s, itype)
        anext_or_next(it, itype)
        # error stored within the generator, never yielded out
        if itype == "async":
            TEST_LOOP.run_until_complete(asyncio.sleep(0.1))
            TEST_LOOP.run_until_complete(cast(ClosableAsyncIterator[int], it).aclose())
        else:
            time.sleep(0.1)
        del it
        # the queued error has been garbage collected (ref count)
        assert not any(isinstance(obj, ValueError) for obj in gc.get_objects())
