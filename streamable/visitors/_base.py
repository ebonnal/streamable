from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from streamable import _stream

V = TypeVar("V")


class Visitor(ABC, Generic[V]):
    # fmt: off
    @abstractmethod
    def visit_stream(self, stream: _stream.stream) -> V: ...
    # fmt: on

    def visit_catch_stream(self, stream: _stream.CatchStream) -> V:
        return self.visit_stream(stream)

    def visit_filter_stream(self, stream: _stream.FilterStream) -> V:
        return self.visit_stream(stream)

    def visit_flatten_stream(self, stream: _stream.FlattenStream) -> V:
        return self.visit_stream(stream)

    def visit_do_stream(self, stream: _stream.DoStream) -> V:
        return self.visit_stream(stream)

    def visit_group_stream(self, stream: _stream.GroupStream) -> V:
        return self.visit_stream(stream)

    def visit_groupby_stream(self, stream: _stream.GroupbyStream) -> V:
        return self.visit_stream(stream)

    def visit_observe_stream(self, stream: _stream.ObserveStream) -> V:
        return self.visit_stream(stream)

    def visit_map_stream(self, stream: _stream.MapStream) -> V:
        return self.visit_stream(stream)

    def visit_skip_stream(self, stream: _stream.SkipStream) -> V:
        return self.visit_stream(stream)

    def visit_throttle_stream(self, stream: _stream.ThrottleStream) -> V:
        return self.visit_stream(stream)

    def visit_head_stream(self, stream: _stream.HeadStream) -> V:
        return self.visit_stream(stream)
