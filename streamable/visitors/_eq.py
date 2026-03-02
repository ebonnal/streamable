from typing import TYPE_CHECKING


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

    def __init__(self, other: object):
        self.other: object = other

    def visit_catch_stream(self, s: "CatchStream") -> bool:
        from streamable._stream import CatchStream

        if not isinstance(self.other, CatchStream):
            return False
        return (
            s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._errors == self.other._errors
            and s._where == self.other._where
            and s._do == self.other._do
            and s._replace == self.other._replace
            and s._stop == self.other._stop
        )

    def visit_do_stream(self, s: "DoStream") -> bool:
        from streamable._stream import DoStream

        if not isinstance(self.other, DoStream):
            return False
        return (
            s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._effect == self.other._effect
            and s._concurrency == self.other._concurrency
            and s._as_completed == self.other._as_completed
        )

    def visit_filter_stream(self, s: "FilterStream") -> bool:
        from streamable._stream import FilterStream

        if not isinstance(self.other, FilterStream):
            return False
        return (
            s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._where == self.other._where
        )

    def visit_flatten_stream(self, s: "FlattenStream") -> bool:
        from streamable._stream import FlattenStream

        if not isinstance(self.other, FlattenStream):
            return False
        return (
            s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._concurrency == self.other._concurrency
        )

    def visit_group_stream(self, s: "GroupStream") -> bool:
        from streamable._stream import GroupStream

        if not isinstance(self.other, GroupStream):
            return False
        return (
            s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._up_to == self.other._up_to
            and s._within == self.other._within
            and s._by == self.other._by
        )

    def visit_map_stream(self, s: "MapStream") -> bool:
        from streamable._stream import MapStream

        if not isinstance(self.other, MapStream):
            return False
        return (
            s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._into == self.other._into
            and s._concurrency == self.other._concurrency
            and s._as_completed == self.other._as_completed
        )

    def visit_observe_stream(self, s: "ObserveStream") -> bool:
        from streamable._stream import ObserveStream

        if not isinstance(self.other, ObserveStream):
            return False
        return (
            s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._subject == self.other._subject
            and s._every == self.other._every
            and s._do == self.other._do
        )

    def visit_skip_stream(self, s: "SkipStream") -> bool:
        from streamable._stream import SkipStream

        if not isinstance(self.other, SkipStream):
            return False
        return (
            s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._until == self.other._until
        )

    def visit_take_stream(self, s: "TakeStream") -> bool:
        from streamable._stream import TakeStream

        if not isinstance(self.other, TakeStream):
            return False
        return (
            s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._until == self.other._until
        )

    def visit_throttle_stream(self, s: "ThrottleStream") -> bool:
        from streamable._stream import ThrottleStream

        if not isinstance(self.other, ThrottleStream):
            return False
        return (
            s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._up_to == self.other._up_to
            and s._per == self.other._per
        )

    def visit_buffer_stream(self, s: "BufferStream") -> bool:
        from streamable._stream import BufferStream

        if not isinstance(self.other, BufferStream):
            return False
        return (
            s.upstream.accept(EqualityVisitor(self.other.upstream))
            and s._up_to == self.other._up_to
        )

    def visit_stream(self, s: "stream") -> bool:
        from streamable._stream import stream

        if not isinstance(self.other, stream):
            return False
        return s.source == self.other.source
