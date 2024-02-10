import unittest
from typing import Any, cast

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
    def test_raises_not_implemented(self) -> None:
        visitor = Visitor[Any]()
        stream = Stream(lambda: range(10))
        with self.assertRaises(NotImplementedError):
            visitor.visit_any(stream)
        with self.assertRaises(NotImplementedError):
            visitor.visit_group_stream(cast(GroupStream, ...))
        with self.assertRaises(NotImplementedError):
            visitor.visit_catch_stream(cast(CatchStream, ...))
        with self.assertRaises(NotImplementedError):
            visitor.visit_filter_stream(cast(FilterStream, ...))
        with self.assertRaises(NotImplementedError):
            visitor.visit_flatten_stream(cast(FlattenStream, ...))
        with self.assertRaises(NotImplementedError):
            visitor.visit_foreach_stream(cast(ForeachStream, ...))
        with self.assertRaises(NotImplementedError):
            visitor.visit_limit_stream(cast(LimitStream, ...))
        with self.assertRaises(NotImplementedError):
            visitor.visit_map_stream(cast(MapStream, ...))
        with self.assertRaises(NotImplementedError):
            visitor.visit_observe_stream(cast(ObserveStream, ...))
        with self.assertRaises(NotImplementedError):
            visitor.visit_slow_stream(cast(SlowStream, ...))
        with self.assertRaises(NotImplementedError):
            visitor.visit_stream(stream)
