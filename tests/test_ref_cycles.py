from datetime import timedelta
from typing import (
    Any,
    Callable,
    Iterator,
    TypeVar,
    cast,
)

import pytest

from streamable import stream
from tests.tools.error import TestError
from tests.tools.func import identity
from tests.tools.gc import disabled_gc, find_object
from tests.tools.iter import (
    ITERABLE_TYPES,
    IterableType,
    aiter_or_iter,
)

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
        # lambda src: stream(cast(Iterator[int], src)).group(1, by=identity, within=timedelta(seconds=1)),
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

        # At this point the error should have been garbage collected (ref count)

        # Get the object if it still exists
        error = find_object(error_id, ValueError)

        assert error is None
