from typing import Any

from streamable._stream import (
    CatchStream,
    FilterStream,
    FlattenStream,
    DoStream,
    GroupbyStream,
    GroupStream,
    MapStream,
    WatchStream,
    SkipStream,
    stream,
    ThrottleStream,
    KeepStream,
)
from streamable.visitors import Visitor


class EqualityVisitor(Visitor[bool]):
    def __init__(self, other: Any):
        self.other: Any = other

    def type_eq(self, stream: stream) -> bool:
        return type(stream) is type(self.other)

    def visit_catch_stream(self, stream: CatchStream) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._errors == self.other._errors
            and stream._when == self.other._when
            and stream._replace == self.other._replace
            and stream._stop == self.other._stop
        )

    def visit_filter_stream(self, stream: FilterStream) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._where == self.other._where
        )

    def visit_flatten_stream(self, stream: FlattenStream) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._concurrency == self.other._concurrency
        )

    def visit_do_stream(self, stream: DoStream) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._concurrency == self.other._concurrency
            and stream._effect == self.other._effect
            and stream._ordered == self.other._ordered
        )

    def visit_group_stream(self, stream: GroupStream) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._by == self.other._by
            and stream._up_to == self.other._up_to
            and stream._every == self.other._every
        )

    def visit_groupby_stream(self, stream: GroupbyStream) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._by == self.other._by
            and stream._up_to == self.other._up_to
            and stream._every == self.other._every
        )

    def visit_map_stream(self, stream: MapStream) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._concurrency == self.other._concurrency
            and stream._into == self.other._into
            and stream._ordered == self.other._ordered
        )

    def visit_watch_stream(self, stream: WatchStream) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._label == self.other._label
            and stream._every == self.other._every
        )

    def visit_skip_stream(self, stream: SkipStream) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._until == self.other._until
        )

    def visit_throttle_stream(self, stream: ThrottleStream) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._up_to == self.other._up_to
            and stream._per == self.other._per
        )

    def visit_keep_stream(self, stream: KeepStream) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._when == self.other._when
        )

    def visit_stream(self, stream: stream) -> bool:
        return self.type_eq(stream) and stream.source == self.other.source
