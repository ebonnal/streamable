from typing import Generic, TypeVar

from streamable import _stream

V = TypeVar("V")


class Visitor(Generic[V]):
    def visit_any(self, stream: _stream.Stream) -> V:
        raise NotImplementedError()

    def visit_batch_stream(self, stream: _stream.BatchStream) -> V:
        return self.visit_any(stream)

    def visit_catch_stream(self, stream: _stream.CatchStream) -> V:
        return self.visit_any(stream)

    def visit_do_stream(self, stream: _stream.DoStream) -> V:
        return self.visit_any(stream)

    def visit_filter_stream(self, stream: _stream.FilterStream) -> V:
        return self.visit_any(stream)

    def visit_flatten_stream(self, stream: _stream.FlattenStream) -> V:
        return self.visit_any(stream)

    def visit_limit_stream(self, stream: _stream.LimitStream) -> V:
        return self.visit_any(stream)

    def visit_observe_stream(self, stream: _stream.ObserveStream) -> V:
        return self.visit_any(stream)

    def visit_map_stream(self, stream: _stream.MapStream) -> V:
        return self.visit_any(stream)

    def visit_slow_stream(self, stream: _stream.SlowStream) -> V:
        return self.visit_any(stream)

    def visit_stream(self, stream: _stream.Stream) -> V:
        return self.visit_any(stream)
