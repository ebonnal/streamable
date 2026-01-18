from datetime import timedelta
import gc
from streamable._tools._func import _Syncified
from typing import (
    Any,
    Callable,
    Iterator,
    cast,
)

import pytest

from streamable import stream
from tests.utils.func import (
    async_identity,
    identity,
)
from tests.utils.error import TestError
from tests.utils.gc import disabled_gc, get_referees
from tests.utils.iter import (
    ITERABLE_TYPES,
    IterableType,
    aiter_or_iter,
    anext_or_next,
)
from tests.utils.source import INTEGERS, ints


@pytest.mark.skip(reason="exceptions lead to reference cycles")
@pytest.mark.parametrize(
    "operate",
    [
        lambda src: stream(cast(Iterator[int], src)).buffer(10),
        lambda src: stream(cast(Iterator[int], src)).catch(TestError),
        lambda src: stream(cast(Iterator[int], src)).buffer(10),
        lambda src: stream(cast(Iterator[int], src)).map(identity, concurrency=2),
        lambda src: stream(cast(Iterator[int], src)).map(identity),
        lambda src: stream(cast(Iterator[int], src)).map(async_identity, concurrency=2),
        lambda src: stream(cast(Iterator[int], src)).map(async_identity),
        lambda src: stream(cast(Iterator[int], src))
        .group(every=timedelta(seconds=1))
        .flatten(concurrency=2),
    ],
)
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_ref_cycles(
    itype: IterableType, operate: Callable[[Iterator[int]], Any]
) -> None:
    with disabled_gc():
        it = aiter_or_iter(operate(map(int, "12-34")), itype)
        # complete iteration, capturing the ValueError's id
        while True:
            try:
                anext_or_next(it, itype)
            except (StopIteration, StopAsyncIteration):
                break
            except ValueError as e:
                error_id = id(e)
        # the error caught has been garbage collected by reference counting
        assert error_id not in map(id, gc.get_objects())


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
