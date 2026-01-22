from datetime import timedelta
import gc
from unittest.mock import Mock, patch
from typing import (
    Any,
    Callable,
    Iterator,
    TypeVar,
    cast,
)

import pytest

from streamable import stream
from streamable.visitors._iter import IteratorVisitor
from tests.utils.func import (
    async_identity,
    identity,
)
from tests.utils.error import TestError
from tests.utils.gc import disabled_gc
from tests.utils.iter import (
    ITERABLE_TYPES,
    IterableType,
    aiter_or_iter,
    anext_or_next,
)
from tests.utils.source import INTEGERS, ints

T = TypeVar("T")


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


def test_attached_loop_auto_closing() -> None:
    """
    The loop attached to the sync iterators involving async functions should be closed when the iteration stops or the iterator is garbage collected.
    """

    visitor: IteratorVisitor = Mock()

    class FakeIteratorVisitor(IteratorVisitor[T]):
        def __init__(self) -> None:
            nonlocal visitor
            super().__init__()
            visitor = self

    with patch("streamable._stream.IteratorVisitor", new=FakeIteratorVisitor):
        # closed on finalisation
        it = iter(ints.filter(async_identity).map(async_identity))
        assert visitor.loop is not None
        assert not visitor.loop.is_closed()
        assert list(it) == list(INTEGERS)[1:]
        assert visitor.loop.is_closed()

        # closed on garbage collection
        it = iter(ints.filter(async_identity).map(async_identity))
        assert visitor.loop is not None
        assert not visitor.loop.is_closed()
        assert next(it) == 1
        del it
        assert visitor.loop.is_closed()
