import unittest
from typing import cast

from streamable.stream import (
    CatchStream,
    FilterStream,
    FlattenStream,
    ForeachStream,
    GroupStream,
    LimitStream,
    MapStream,
    ObserveStream,
    SlowStream,
    Stream,
)
from streamable.visitor import Visitor


class TestVisitor(unittest.TestCase):
    def test_visitor(self) -> None:
        class ConcreteVisitor(Visitor[None]):
            def visit_stream(self, stream: Stream) -> None:
                return None

        visitor = ConcreteVisitor()
        visitor.visit_group_stream(cast(GroupStream, ...))
        visitor.visit_catch_stream(cast(CatchStream, ...))
        visitor.visit_filter_stream(cast(FilterStream, ...))
        visitor.visit_flatten_stream(cast(FlattenStream, ...))
        visitor.visit_foreach_stream(cast(ForeachStream, ...))
        visitor.visit_limit_stream(cast(LimitStream, ...))
        visitor.visit_map_stream(cast(MapStream, ...))
        visitor.visit_observe_stream(cast(ObserveStream, ...))
        visitor.visit_slow_stream(cast(SlowStream, ...))
        visitor.visit_stream(cast(Stream, ...))
