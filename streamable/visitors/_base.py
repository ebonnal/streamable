from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from streamable import _stream

V = TypeVar("V")


class Visitor(ABC, Generic[V]):
    # fmt: off
    @abstractmethod
    def visit_stream(self, stream: _stream.Stream) -> V: ...
    # fmt: on

    def visit_catch_stream(self, stream: _stream.CatchStream) -> V:
        return self.visit_stream(stream)

    def visit_acatch_stream(self, stream: _stream.ACatchStream) -> V:
        return self.visit_stream(stream)

    def visit_distinct_stream(self, stream: _stream.DistinctStream) -> V:
        return self.visit_stream(stream)

    def visit_filter_stream(self, stream: _stream.FilterStream) -> V:
        return self.visit_stream(stream)

    def visit_flatten_stream(self, stream: _stream.FlattenStream) -> V:
        return self.visit_stream(stream)

    def visit_aflatten_stream(self, stream: _stream.AFlattenStream) -> V:
        return self.visit_stream(stream)

    def visit_foreach_stream(self, stream: _stream.ForeachStream) -> V:
        return self.visit_stream(stream)

    def visit_aforeach_stream(self, stream: _stream.AForeachStream) -> V:
        return self.visit_stream(stream)

    def visit_group_stream(self, stream: _stream.GroupStream) -> V:
        return self.visit_stream(stream)

    def visit_agroup_stream(self, stream: _stream.AGroupStream) -> V:
        return self.visit_stream(stream)

    def visit_groupby_stream(self, stream: _stream.GroupbyStream) -> V:
        return self.visit_stream(stream)

    def visit_agroupby_stream(self, stream: _stream.AGroupbyStream) -> V:
        return self.visit_stream(stream)

    def visit_observe_stream(self, stream: _stream.ObserveStream) -> V:
        return self.visit_stream(stream)

    def visit_map_stream(self, stream: _stream.MapStream) -> V:
        return self.visit_stream(stream)

    def visit_amap_stream(self, stream: _stream.AMapStream) -> V:
        return self.visit_stream(stream)

    def visit_skip_stream(self, stream: _stream.SkipStream) -> V:
        return self.visit_stream(stream)

    def visit_throttle_stream(self, stream: _stream.ThrottleStream) -> V:
        return self.visit_stream(stream)

    def visit_truncate_stream(self, stream: _stream.TruncateStream) -> V:
        return self.visit_stream(stream)
