from typing import cast

from streamable._stream import (
    CatchStream,
    DoStream,
    FilterStream,
    FlattenStream,
    GroupStream,
    MapStream,
    ObserveStream,
    SkipStream,
    stream,
    ThrottleStream,
    TakeStream,
)
from streamable.visitors import Visitor


def test_visitor() -> None:
    class ConcreteVisitor(Visitor[None]):
        def visit_stream(self, stream: stream) -> None:
            return None

    visitor = ConcreteVisitor()
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


def test_depth_visitor_example():
    from streamable.visitors import Visitor

    class DepthVisitor(Visitor[int]):
        def visit_stream(self, stream: stream) -> int:
            if not stream.upstream:
                return 1
            return 1 + stream.upstream.accept(self)

    def depth(stream: stream) -> int:
        return stream.accept(DepthVisitor())

    assert depth(stream(range(10)).map(str).do(print)) == 3
