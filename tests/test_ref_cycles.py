import asyncio
import gc
import time
import weakref
from datetime import timedelta
from typing import (
    Any,
    AsyncIterable,
    Callable,
    Iterable,
    Iterator,
    List,
    TypeVar,
    cast,
)

import pytest

from streamable import stream
from streamable._tools._iter import ClosableAsyncIterator
from tests.tools.error import TestError
from tests.tools.func import identity
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


@pytest.mark.parametrize(
    "operate",
    [
        lambda src: stream(cast(Iterator[int], src)).buffer(10),
        lambda src: stream(cast(Iterator[int], src)).catch(TestError),
        lambda src: stream(cast(Iterator[int], src)).do(identity),
        lambda src: stream(cast(Iterator[int], src)).filter(),
        lambda src: stream(cast(Iterator[int], src)).group(2).flatten(),
        lambda src: stream(cast(Iterator[int], src)).group(2).flatten(concurrency=2),
        lambda src: stream(cast(Iterator[int], src)).group(2).flatten(concurrency=10),
        lambda src: stream(cast(Iterator[int], src)).group(1, by=identity).flatten(),
        lambda src: stream(cast(Iterator[int], src)).group(
            1, by=identity, within=timedelta(seconds=1)
        ),
        lambda src: stream(cast(Iterator[int], src)).map(identity),
        lambda src: stream(cast(Iterator[int], src)).map(identity, concurrency=2),
        lambda src: stream(cast(Iterator[int], src)).map(identity, concurrency=10),
        lambda src: stream(cast(Iterator[int], src)).map(
            identity, concurrency=2, as_completed=True
        ),
        lambda src: stream(cast(Iterator[int], src)).map(
            identity, concurrency=10, as_completed=True
        ),
        lambda src: stream(cast(Iterator[int], src)).observe(),
        lambda src: stream(cast(Iterator[int], src)).observe(every=1000),
        lambda src: stream(cast(Iterator[int], src)).observe(
            every=timedelta(seconds=1)
        ),
        lambda src: stream(cast(Iterator[int], src)).skip(10),
        lambda src: stream(cast(Iterator[int], src)).take(10),
        lambda src: stream(cast(Iterator[int], src)).throttle(
            10, per=timedelta(seconds=1)
        ),
        # errors raised by the mapped function / a flattened inner iterator
        lambda _: stream(iter("123-")).map(int),
        lambda _: stream(iter("123-")).map(int, concurrency=2),
        lambda _: stream(iter("123-")).map(int, concurrency=2, as_completed=True),
        lambda _: stream(iter("123-")).group(2).map(lambda s: map(int, s)).flatten(),
        lambda _: stream(iter("123-"))
        .group(2)
        .map(lambda s: map(int, s))
        .flatten(concurrency=2),
        lambda _: stream(iter("123-"))
        .group(2)
        .map(lambda s: stream(s).map(int))
        .flatten(),
        lambda _: stream(iter("123-"))
        .group(2)
        .map(lambda s: stream(s).map(int))
        .flatten(concurrency=2),
    ],
)
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
@pytest.mark.asyncio
async def test_ref_cycles(
    itype: IterableType, operate: Callable[[Iterator[int]], Any]
) -> None:
    with disabled_gc():
        it = aiter_or_iter(operate(map(int, "123-")), itype)
        # complete iteration, capturing the ValueError's id
        while True:
            try:
                if isinstance(it, Iterator):
                    it.__next__()
                else:
                    await it.__anext__()
            except ValueError as e:
                error_id = id(e)
                break

        # let the event loop run the callbacks still referencing the futures
        await asyncio.sleep(0.01)

        # At this point the error should have been garbage collected (ref count)

        # Get the object if it still exists
        error = find_object(error_id, ValueError)

        # objgraph.show_backrefs([error], filename="cycles.png", max_depth=10)

        assert error is None


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_ref_cycles_exhausted_flattened_iterators(itype: IterableType) -> None:
    with disabled_gc():
        iterators: List[Iterable[int]] = [(n for n in [0]), (n for n in [1])]
        refs = [weakref.ref(iterator) for iterator in iterators]
        assert alist_or_list(stream(iterators).flatten(concurrency=2), itype) == [0, 1]
        del iterators
        # the exhausted iterators have been garbage collected (ref count)
        assert not any(ref() for ref in refs)


@pytest.mark.parametrize("as_completed", [False, True])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_ref_cycles_in_flight_errors(
    itype: IterableType, as_completed: bool, caplog: pytest.LogCaptureFixture
) -> None:
    def slow_int(s: str) -> int:
        time.sleep(0.01 if s == "-" else 0)
        return int(s)

    gc.collect()
    with disabled_gc():
        s = stream(iter("1---")).map(slow_int, concurrency=4, as_completed=as_completed)
        it = aiter_or_iter(s, itype)
        assert anext_or_next(it, itype) == 1
        # the 3 failing tasks are done (running the loop: the async results get set), their errors are in flight
        TEST_LOOP.run_until_complete(asyncio.sleep(0.05))
        if itype is AsyncIterable:
            TEST_LOOP.run_until_complete(cast(ClosableAsyncIterator[int], it).aclose())
        del it
        # the in-flight errors have been garbage collected (ref count)
        assert not any(isinstance(obj, ValueError) for obj in gc.get_objects())
    # no "Future exception was never retrieved"
    assert not caplog.records
