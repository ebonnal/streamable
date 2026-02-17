from abc import ABC
import logging
from typing import TYPE_CHECKING, List

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


class ReprVisitor(Visitor[str], ABC):
    __slots__ = ("operation_reprs", "max_len")

    def __init__(self, max_len: int = 80) -> None:
        self.operation_reprs: List[str] = []
        self.max_len = max_len

    def visit_buffer_stream(self, s: "BufferStream") -> str:
        self.operation_reprs.append(f"buffer({self.to_string(s._up_to)})")
        return s.upstream.accept(self)

    def visit_catch_stream(self, s: "CatchStream") -> str:
        if isinstance(s._errors, tuple):
            errors = f"({', '.join(map(self.to_string, s._errors))})"
        else:
            errors = self.to_string(s._errors)
        self.operation_reprs.append(
            f"catch({errors}, where={self.to_string(s._where)}, do={self.to_string(s._do)}, replace={self.to_string(s._replace)}, stop={self.to_string(s._stop)})"
        )
        return s.upstream.accept(self)

    def visit_do_stream(self, s: "DoStream") -> str:
        self.operation_reprs.append(
            f"do({self.to_string(s._effect)}, concurrency={self.to_string(s._concurrency)}, as_completed={self.to_string(s._as_completed)})"
        )
        return s.upstream.accept(self)

    def visit_filter_stream(self, s: "FilterStream") -> str:
        self.operation_reprs.append(f"filter({self.to_string(s._where)})")
        return s.upstream.accept(self)

    def visit_flatten_stream(self, s: "FlattenStream") -> str:
        self.operation_reprs.append(
            f"flatten(concurrency={self.to_string(s._concurrency)})"
        )
        return s.upstream.accept(self)

    def visit_group_stream(self, s: "GroupStream") -> str:
        self.operation_reprs.append(
            f"group(up_to={self.to_string(s._up_to)}, within={self.to_string(s._within)}, by={self.to_string(s._by)})"
        )
        return s.upstream.accept(self)

    def visit_map_stream(self, s: "MapStream") -> str:
        self.operation_reprs.append(
            f"map({self.to_string(s._into)}, concurrency={self.to_string(s._concurrency)}, as_completed={self.to_string(s._as_completed)})"
        )
        return s.upstream.accept(self)

    def visit_observe_stream(self, s: "ObserveStream") -> str:
        do = (
            f", do={self.to_string(s._do)}"
            if s._do != logging.getLogger("streamable").info
            else ""
        )
        self.operation_reprs.append(
            f"""observe({self.to_string(s._subject)}, every={self.to_string(s._every)}{do})"""
        )
        return s.upstream.accept(self)

    def visit_skip_stream(self, s: "SkipStream") -> str:
        self.operation_reprs.append(f"skip(until={self.to_string(s._until)})")
        return s.upstream.accept(self)

    def visit_take_stream(self, s: "TakeStream") -> str:
        self.operation_reprs.append(f"take(until={self.to_string(s._until)})")
        return s.upstream.accept(self)

    def visit_throttle_stream(self, s: "ThrottleStream") -> str:
        self.operation_reprs.append(
            f"throttle({self.to_string(s._up_to)}, per={self.to_string(s._per)})"
        )
        return s.upstream.accept(self)

    def visit_stream(self, s: "stream") -> str:
        source_stream = f"stream({self.to_string(s.source)})"
        depth = len(self.operation_reprs) + 1
        if depth == 1:
            return source_stream
        one_liner_repr = f"{source_stream}.{'.'.join(reversed(self.operation_reprs))}"
        if len(one_liner_repr) <= self.max_len:
            return one_liner_repr
        operations_repr = "".join(
            f"    .{operation_repr}\n"
            for operation_repr in reversed(self.operation_reprs)
        )
        return f"(\n    {source_stream}\n{operations_repr})"

    @classmethod
    def to_string(cls, o: object) -> str:
        if repr(o).startswith("<"):
            return getattr(o, "__name__", repr(o))
        return repr(o)
