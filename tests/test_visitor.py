import unittest
from typing import cast

from streamable.stream import (
    ACatchStream,
    ADistinctStream,
    AFilterStream,
    AFlattenStream,
    AForeachStream,
    AGroupbyStream,
    AGroupStream,
    AMapStream,
    ASkipStream,
    ATruncateStream,
    CatchStream,
    DistinctStream,
    FilterStream,
    FlattenStream,
    ForeachStream,
    GroupbyStream,
    GroupStream,
    MapStream,
    ObserveStream,
    SkipStream,
    Stream,
    ThrottleStream,
    TruncateStream,
)
from streamable.visitors import Visitor


class TestVisitor(unittest.TestCase):
    def test_visitor(self) -> None:
        class ConcreteVisitor(Visitor[None]):
            def visit_stream(self, stream: Stream) -> None:
                return None

        visitor = ConcreteVisitor()
        visitor.visit_catch_stream(cast(CatchStream, ...))
        visitor.visit_acatch_stream(cast(ACatchStream, ...))
        visitor.visit_distinct_stream(cast(DistinctStream, ...))
        visitor.visit_adistinct_stream(cast(ADistinctStream, ...))
        visitor.visit_filter_stream(cast(FilterStream, ...))
        visitor.visit_afilter_stream(cast(AFilterStream, ...))
        visitor.visit_flatten_stream(cast(FlattenStream, ...))
        visitor.visit_aflatten_stream(cast(AFlattenStream, ...))
        visitor.visit_foreach_stream(cast(ForeachStream, ...))
        visitor.visit_aforeach_stream(cast(AForeachStream, ...))
        visitor.visit_group_stream(cast(GroupStream, ...))
        visitor.visit_agroup_stream(cast(AGroupStream, ...))
        visitor.visit_groupby_stream(cast(GroupbyStream, ...))
        visitor.visit_agroupby_stream(cast(AGroupbyStream, ...))
        visitor.visit_map_stream(cast(MapStream, ...))
        visitor.visit_amap_stream(cast(AMapStream, ...))
        visitor.visit_observe_stream(cast(ObserveStream, ...))
        visitor.visit_skip_stream(cast(SkipStream, ...))
        visitor.visit_askip_stream(cast(ASkipStream, ...))
        visitor.visit_throttle_stream(cast(ThrottleStream, ...))
        visitor.visit_truncate_stream(cast(TruncateStream, ...))
        visitor.visit_atruncate_stream(cast(ATruncateStream, ...))
        visitor.visit_stream(cast(Stream, ...))

    def test_depth_visitor_example(self):
        from streamable.visitors import Visitor

        class DepthVisitor(Visitor[int]):
            def visit_stream(self, stream: Stream) -> int:
                if not stream.upstream:
                    return 1
                return 1 + stream.upstream.accept(self)

        def depth(stream: Stream) -> int:
            return stream.accept(DepthVisitor())

        self.assertEqual(
            depth(Stream(range(10)).map(str).foreach(print)),
            3,
            msg="DepthVisitor example should work",
        )
