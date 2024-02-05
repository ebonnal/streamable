from typing import Generic, TypeVar

from streamable import stream

V = TypeVar("V")


class Visitor(Generic[V]):
    def visit_any(self, stream: stream.Stream) -> V:
        raise NotImplementedError()

    def visit_batch_stream(self, stream: stream.BatchStream) -> V:
        return self.visit_any(stream)

    def visit_catch_stream(self, stream: stream.CatchStream) -> V:
        return self.visit_any(stream)

    def visit_filter_stream(self, stream: stream.FilterStream) -> V:
        return self.visit_any(stream)

    def visit_flatten_stream(self, stream: stream.FlattenStream) -> V:
        return self.visit_any(stream)

    def visit_foreach_stream(self, stream: stream.ForeachStream) -> V:
        return self.visit_any(stream)

    def visit_limit_stream(self, stream: stream.LimitStream) -> V:
        return self.visit_any(stream)

    def visit_observe_stream(self, stream: stream.ObserveStream) -> V:
        return self.visit_any(stream)

    def visit_map_stream(self, stream: stream.MapStream) -> V:
        return self.visit_any(stream)

    def visit_slow_stream(self, stream: stream.SlowStream) -> V:
        return self.visit_any(stream)

    def visit_stream(self, stream: stream.Stream) -> V:
        return self.visit_any(stream)
