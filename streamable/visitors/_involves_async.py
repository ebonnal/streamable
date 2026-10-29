from inspect import iscoroutinefunction
from typing import TYPE_CHECKING, AsyncIterable, Iterable

from streamable.visitors._base import Visitor

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
        TakeStream,
        ThrottleStream,
        stream,
    )


class InvolvesAsyncVisitor(Visitor[bool]):
    def visit_buffer_stream(self, s: "BufferStream") -> bool:
        return s.upstream.accept(self)

    def visit_catch_stream(self, s: "CatchStream") -> bool:
        return (
            iscoroutinefunction(s._where)
            or iscoroutinefunction(s._replace)
            or iscoroutinefunction(s._do)
        ) or s.upstream.accept(self)

    def visit_do_stream(self, s: "DoStream") -> bool:
        return iscoroutinefunction(s._effect) or s.upstream.accept(self)

    def visit_filter_stream(self, s: "FilterStream") -> bool:
        return iscoroutinefunction(s._where) or s.upstream.accept(self)

    def visit_flatten_stream(self, s: "FlattenStream") -> bool:
        return s.upstream.accept(self)

    def visit_group_stream(self, s: "GroupStream") -> bool:
        return iscoroutinefunction(s._by) or s.upstream.accept(self)

    def visit_map_stream(self, s: "MapStream") -> bool:
        return iscoroutinefunction(s._into) or s.upstream.accept(self)

    def visit_observe_stream(self, s: "ObserveStream") -> bool:
        return iscoroutinefunction(s._do) or s.upstream.accept(self)

    def visit_skip_stream(self, s: "SkipStream") -> bool:
        return iscoroutinefunction(s._until) or s.upstream.accept(self)

    def visit_take_stream(self, s: "TakeStream") -> bool:
        return iscoroutinefunction(s._until) or s.upstream.accept(self)

    def visit_throttle_stream(self, s: "ThrottleStream") -> bool:
        return s.upstream.accept(self)

    def visit_stream(self, s: "stream") -> bool:
        from streamable._stream import stream

        if isinstance(s.source, stream):
            return s.source.accept(self)
        is_only_async_iterable = isinstance(s.source, AsyncIterable) and not isinstance(
            s.source, Iterable
        )
        return is_only_async_iterable or iscoroutinefunction(s.source)
