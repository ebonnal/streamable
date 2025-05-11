from typing import Any, Union

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


class EqualityVisitor(Visitor[bool]):
    def __init__(self, other: Any):
        self.other: Any = other

    def type_eq(self, stream: Stream) -> bool:
        return type(stream) == type(self.other)

    def catch_eq(self, stream: Union[CatchStream, ACatchStream]) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._errors == self.other._errors
            and stream._when == self.other._when
            and stream._replacement == self.other._replacement
            and stream._finally_raise == self.other._finally_raise
        )

    def visit_catch_stream(self, stream: CatchStream) -> bool:
        return self.catch_eq(stream)

    def visit_acatch_stream(self, stream: ACatchStream) -> bool:
        return self.catch_eq(stream)

    def distinct_eq(self, stream: Union[DistinctStream, ADistinctStream]) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._key == self.other._key
            and stream._consecutive_only == self.other._consecutive_only
        )

    def visit_distinct_stream(self, stream: DistinctStream) -> bool:
        return self.distinct_eq(stream)

    def visit_adistinct_stream(self, stream: ADistinctStream) -> bool:
        return self.distinct_eq(stream)

    def filter_eq(self, stream: Union[FilterStream, AFilterStream]) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._when == self.other._when
        )

    def visit_filter_stream(self, stream: FilterStream) -> bool:
        return self.filter_eq(stream)

    def visit_afilter_stream(self, stream: AFilterStream) -> bool:
        return self.filter_eq(stream)

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
            and stream._effect == self.other._effect
            and stream._ordered == self.other._ordered
        )

    def visit_foreach_stream(self, stream: ForeachStream) -> bool:
        return self.foreach_eq(stream) and stream._via == self.other._via

    def visit_aforeach_stream(self, stream: AForeachStream) -> bool:
        return self.foreach_eq(stream)

    def group_eq(self, stream: Union[GroupStream, AGroupStream]) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._by == self.other._by
            and stream._size == self.other._size
            and stream._interval == self.other._interval
        )

    def visit_group_stream(self, stream: GroupStream) -> bool:
        return self.group_eq(stream)

    def visit_agroup_stream(self, stream: AGroupStream) -> bool:
        return self.group_eq(stream)

    def groupby_eq(self, stream: Union[GroupbyStream, AGroupbyStream]) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._key == self.other._key
            and stream._size == self.other._size
            and stream._interval == self.other._interval
        )

    def visit_groupby_stream(self, stream: GroupbyStream) -> bool:
        return self.groupby_eq(stream)

    def visit_agroupby_stream(self, stream: AGroupbyStream) -> bool:
        return self.groupby_eq(stream)

    def map_eq(self, stream: Union[MapStream, AMapStream]) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._concurrency == self.other._concurrency
            and stream._transformation == self.other._transformation
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

    def skip_eq(self, stream: Union[SkipStream, ASkipStream]) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._count == self.other._count
            and stream._until == self.other._until
        )

    def visit_skip_stream(self, stream: SkipStream) -> bool:
        return self.skip_eq(stream)

    def visit_askip_stream(self, stream: ASkipStream) -> bool:
        return self.skip_eq(stream)

    def visit_throttle_stream(self, stream: ThrottleStream) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._count == self.other._count
            and stream._per == self.other._per
        )

    def truncate_eq(self, stream: Union[TruncateStream, ATruncateStream]) -> bool:
        return (
            self.type_eq(stream)
            and stream.upstream.accept(EqualityVisitor(self.other.upstream))
            and stream._count == self.other._count
            and stream._when == self.other._when
        )

    def visit_truncate_stream(self, stream: TruncateStream) -> bool:
        return self.truncate_eq(stream)

    def visit_atruncate_stream(self, stream: ATruncateStream) -> bool:
        return self.truncate_eq(stream)

    def visit_stream(self, stream: Stream) -> bool:
        return self.type_eq(stream) and stream.source == self.other.source
