from typing import Any, TypeVar

from streamable.stream import (
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

T = TypeVar("T")
U = TypeVar("U")


class EqualityVisitor(Visitor[bool]):
    def __init__(self, other: Any):
        self.other: Any = other

    def visit_catch_stream(self, stream: CatchStream[T]) -> bool:
        return (
            isinstance(self.other, CatchStream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._errors == self.other._errors
            and stream._when == self.other._when
            and stream._replacement == self.other._replacement
            and stream._finally_raise == self.other._finally_raise
        )

    def visit_distinct_stream(self, stream: DistinctStream[T]) -> bool:
        return (
            isinstance(self.other, DistinctStream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._key == self.other._key
            and stream._consecutive_only == self.other._consecutive_only
        )

    def visit_filter_stream(self, stream: FilterStream[T]) -> bool:
        return (
            isinstance(self.other, FilterStream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._when == self.other._when
        )

    def visit_flatten_stream(self, stream: FlattenStream[T]) -> bool:
        return (
            isinstance(self.other, FlattenStream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._concurrency == self.other._concurrency
        )

    def visit_foreach_stream(self, stream: ForeachStream[T]) -> bool:
        return (
            isinstance(self.other, ForeachStream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._concurrency == self.other._concurrency
            and stream._effect == self.other._effect
            and stream._ordered == self.other._ordered
            and stream._via == self.other._via
        )

    def visit_aforeach_stream(self, stream: AForeachStream[T]) -> bool:
        return (
            isinstance(self.other, AForeachStream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._concurrency == self.other._concurrency
            and stream._effect == self.other._effect
            and stream._ordered == self.other._ordered
        )

    def visit_group_stream(self, stream: GroupStream[U]) -> bool:
        return (
            isinstance(self.other, GroupStream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._by == self.other._by
            and stream._size == self.other._size
            and stream._interval == self.other._interval
        )

    def visit_groupby_stream(self, stream: GroupbyStream[U, T]) -> bool:
        return (
            isinstance(self.other, GroupbyStream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._key == self.other._key
            and stream._size == self.other._size
            and stream._interval == self.other._interval
        )

    def visit_map_stream(self, stream: MapStream[U, T]) -> bool:
        return (
            isinstance(self.other, MapStream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._concurrency == self.other._concurrency
            and stream._transformation == self.other._transformation
            and stream._ordered == self.other._ordered
            and stream._via == self.other._via
        )

    def visit_amap_stream(self, stream: AMapStream[U, T]) -> bool:
        return (
            isinstance(self.other, AMapStream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._concurrency == self.other._concurrency
            and stream._transformation == self.other._transformation
            and stream._ordered == self.other._ordered
        )

    def visit_observe_stream(self, stream: ObserveStream[T]) -> bool:
        return (
            isinstance(self.other, ObserveStream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._what == self.other._what
        )

    def visit_skip_stream(self, stream: SkipStream[T]) -> bool:
        return (
            isinstance(self.other, SkipStream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._count == self.other._count
            and stream._until == self.other._until
        )

    def visit_throttle_stream(self, stream: ThrottleStream[T]) -> bool:
        return (
            isinstance(self.other, ThrottleStream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._count == self.other._count
            and stream._per == self.other._per
        )

    def visit_truncate_stream(self, stream: TruncateStream[T]) -> bool:
        return (
            isinstance(self.other, TruncateStream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._count == self.other._count
            and stream._when == self.other._when
        )

    def visit_stream(self, stream: Stream[T]) -> bool:
        return isinstance(self.other, Stream) and stream.source == self.other.source
