import asyncio
from typing import AsyncIterator, Iterator, cast

import pytest

from streamable._stream import (
    BufferStream,
    CatchStream,
    DoStream,
    FilterStream,
    FlattenStream,
    GroupStream,
    MapStream,
    ObserveStream,
    SkipStream,
    TakeStream,
    ThrottleStream,
    stream,
)
from streamable._tools._iter import SyncAsyncIterable
from streamable.visitors import Visitor
from streamable.visitors._involves_async import InvolvesAsyncVisitor


def test_visitor() -> None:
    class ConcreteVisitor(Visitor[None]):
        def visit_stream(self, s: stream) -> None:
            return None

    visitor = ConcreteVisitor()
    visitor.visit_buffer_stream(cast(BufferStream, ...))
    visitor.visit_catch_stream(cast(CatchStream, ...))
    visitor.visit_filter_stream(cast(FilterStream, ...))
    visitor.visit_flatten_stream(cast(FlattenStream, ...))
    visitor.visit_do_stream(cast(DoStream, ...))
    visitor.visit_group_stream(cast(GroupStream, ...))
    visitor.visit_map_stream(cast(MapStream, ...))
    visitor.visit_observe_stream(cast(ObserveStream, ...))
    visitor.visit_skip_stream(cast(SkipStream, ...))
    visitor.visit_throttle_stream(cast(ThrottleStream, ...))
    visitor.visit_take_stream(cast(TakeStream, ...))
    visitor.visit_stream(cast(stream, ...))


def test_depth_visitor_example() -> None:
    from streamable.visitors import Visitor

    class DepthVisitor(Visitor[int]):
        def visit_stream(self, s: stream) -> int:
            if not s.upstream:
                return 1
            return 1 + s.upstream.accept(self)

    def depth(stream: stream) -> int:
        return stream.accept(DepthVisitor())

    assert depth(stream(range(10)).map(str).do(print)) == 3


class DummySyncAsyncIterable(SyncAsyncIterable[int]):
    def __iter__(self) -> Iterator[int]:
        yield 0

    async def __aiter__(self) -> AsyncIterator[int]:
        yield 0


@pytest.mark.parametrize(
    "s, expected_involves_async",
    [
        # INVOLVES ASYNC
        # source is an async iterable only (not sync iterable)
        (stream(stream(range(10)).__aiter__()), True),
        # source is an async function
        (stream(asyncio.Queue().get), True),
        # source is a stream involving async
        (stream(stream(range(10)).do(asyncio.sleep)), True),
        # DOES NOT INVOLVE ASYNC
        # source is a sync function
        (stream(lambda: 0), False),
        # source is a both a sync and an async iterable -> prefer sync
        (stream(DummySyncAsyncIterable()), False),
        # source is a stream not involving async
        (stream(stream(range(10))), False),
    ],
)
def test_visitor_involves_async(s: stream, expected_involves_async: bool) -> None:
    assert InvolvesAsyncVisitor().visit_stream(s) is expected_involves_async
