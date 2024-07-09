from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from streamable import stream

V = TypeVar("V")


class Visitor(ABC, Generic[V]):
    # fmt: off
    @abstractmethod
    def visit_stream(self, stream: stream.Stream) -> V: ...
    # fmt: on

    def visit_catch_stream(self, stream: stream.CatchStream) -> V:
        return self.visit_stream(stream)

    def visit_filter_stream(self, stream: stream.FilterStream) -> V:
        return self.visit_stream(stream)

    def visit_flatten_stream(self, stream: stream.FlattenStream) -> V:
        return self.visit_stream(stream)

    def visit_foreach_stream(self, stream: stream.ForeachStream) -> V:
        return self.visit_stream(stream)

    def visit_aforeach_stream(self, stream: stream.AForeachStream) -> V:
        return self.visit_stream(stream)

    def visit_group_stream(self, stream: stream.GroupStream) -> V:
        return self.visit_stream(stream)

    def visit_truncate_stream(self, stream: stream.TruncateStream) -> V:
        return self.visit_stream(stream)

    def visit_observe_stream(self, stream: stream.ObserveStream) -> V:
        return self.visit_stream(stream)

    def visit_map_stream(self, stream: stream.MapStream) -> V:
        return self.visit_stream(stream)

    def visit_amap_stream(self, stream: stream.AMapStream) -> V:
        return self.visit_stream(stream)

    def visit_throttle_stream(self, stream: stream.ThrottleStream) -> V:
        return self.visit_stream(stream)
