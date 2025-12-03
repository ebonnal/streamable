from typing import Any, Union

from streamable._stream import (
    AFlattenStream,
    AForeachStream,
    AMapStream,
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


class EqualityVisitor(Visitor[bool]):
    def __init__(self, other: Any):
        self.other: Any = other

    def type_eq(self, stream: Stream) -> bool:
        return type(stream) is type(self.other)

    def visit_catch_stream(self, stream: CatchStream) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._errors == self.other._errors
            and stream._when == self.other._when
            and stream._replace == self.other._replace
            and stream._finally_raise == self.other._finally_raise
        )

    def visit_distinct_stream(self, stream: DistinctStream) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._by == self.other._by
            and stream._consecutive == self.other._consecutive
        )

    def visit_filter_stream(self, stream: FilterStream) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._where == self.other._where
        )

    def flatten_eq(self, stream: Union[FlattenStream, AFlattenStream]) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._concurrency == self.other._concurrency
        )

    def visit_flatten_stream(self, stream: FlattenStream) -> bool:
        return self.flatten_eq(stream)

    def visit_aflatten_stream(self, stream: AFlattenStream) -> bool:
        return self.flatten_eq(stream)

    def foreach_eq(self, stream: Union[ForeachStream, AForeachStream]) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._concurrency == self.other._concurrency
            and stream._do == self.other._do
            and stream._ordered == self.other._ordered
        )

    def visit_foreach_stream(self, stream: ForeachStream) -> bool:
        return self.foreach_eq(stream) and stream._via == self.other._via

    def visit_aforeach_stream(self, stream: AForeachStream) -> bool:
        return self.foreach_eq(stream)

    def visit_group_stream(self, stream: GroupStream) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._by == self.other._by
            and stream._size == self.other._size
            and stream._interval == self.other._interval
        )

    def visit_groupby_stream(self, stream: GroupbyStream) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._key == self.other._key
            and stream._size == self.other._size
            and stream._interval == self.other._interval
        )

    def map_eq(self, stream: Union[MapStream, AMapStream]) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._concurrency == self.other._concurrency
            and stream._to == self.other._to
            and stream._ordered == self.other._ordered
        )

    def visit_map_stream(self, stream: MapStream) -> bool:
        return self.map_eq(stream) and stream._via == self.other._via

    def visit_amap_stream(self, stream: AMapStream) -> bool:
        return self.map_eq(stream)

    def visit_observe_stream(self, stream: ObserveStream) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._what == self.other._what
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

    def visit_truncate_stream(self, stream: TruncateStream) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._when == self.other._when
        )

    def visit_stream(self, stream: Stream) -> bool:
        return self.type_eq(stream) and stream.source == self.other.source
