from typing import TYPE_CHECKING, Any


from streamable.visitors import Visitor

if TYPE_CHECKING:  # pragma: no cover
    from streamable._stream import (
        BufferStream,
        CatchStream,
        DoStream,
        FilterStream,
        FlattenStream,
        GroupStream,
        MapStream,
        ObserveStream,
        SkipStream,
        stream,
        ThrottleStream,
        TakeStream,
    )


class EqualityVisitor(Visitor[bool]):
    __slots__ = ("other",)

    def __init__(self, other: Any):
        self.other: Any = other

    def type_eq(self, s: "stream") -> bool:
        return type(s) is type(self.other)

    def visit_catch_stream(self, s: "CatchStream") -> bool:
        return (
            self.type_eq(s)
            and s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._errors == self.other._errors
            and s._where == self.other._where
            and s._do == self.other._do
            and s._replace == self.other._replace
            and s._stop == self.other._stop
        )

    def visit_do_stream(self, s: "DoStream") -> bool:
        return (
            self.type_eq(s)
            and s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._effect == self.other._effect
            and s._concurrency == self.other._concurrency
            and s._as_completed == self.other._as_completed
        )

    def visit_filter_stream(self, s: "FilterStream") -> bool:
        return (
            self.type_eq(s)
            and s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._where == self.other._where
        )

    def visit_flatten_stream(self, s: "FlattenStream") -> bool:
        return (
            self.type_eq(s)
            and s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._concurrency == self.other._concurrency
        )

    def visit_group_stream(self, s: "GroupStream") -> bool:
        return (
            self.type_eq(s)
            and s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._up_to == self.other._up_to
            and s._within == self.other._within
            and s._by == self.other._by
        )

    def visit_map_stream(self, s: "MapStream") -> bool:
        return (
            self.type_eq(s)
            and s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._into == self.other._into
            and s._concurrency == self.other._concurrency
            and s._as_completed == self.other._as_completed
        )

    def visit_observe_stream(self, s: "ObserveStream") -> bool:
        return (
            self.type_eq(s)
            and s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._subject == self.other._subject
            and s._every == self.other._every
            and s._do == self.other._do
        )

    def visit_skip_stream(self, s: "SkipStream") -> bool:
        return (
            self.type_eq(s)
            and s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._until == self.other._until
        )

    def visit_take_stream(self, s: "TakeStream") -> bool:
        return (
            self.type_eq(s)
            and s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._until == self.other._until
        )

    def visit_throttle_stream(self, s: "ThrottleStream") -> bool:
        return (
            self.type_eq(s)
            and s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._up_to == self.other._up_to
            and s._per == self.other._per
        )

    def visit_buffer_stream(self, s: "BufferStream") -> bool:
        return (
            self.type_eq(s)
            and s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._up_to == self.other._up_to
        )

    def visit_stream(self, s: "stream") -> bool:
        return self.type_eq(s) and s.source == self.other.source
