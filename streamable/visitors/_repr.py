from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, List

from streamable._utils._func import _Star
from streamable.visitors import Visitor

if TYPE_CHECKING:  # pragma: no cover
    from streamable._stream import (
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


class ToStringVisitor(Visitor[str], ABC):
    def __init__(self, max_len: int = 80) -> None:
        self.operation_reprs: List[str] = []
        self.max_len = max_len

    @staticmethod
    @abstractmethod
    def to_string(o: object) -> str: ...

    def visit_catch_stream(self, stream: "CatchStream") -> str:
        if isinstance(stream._errors, tuple):
            errors = f"({', '.join(map(self.to_string, stream._errors))})"
        else:
            errors = self.to_string(stream._errors)
        self.operation_reprs.append(
            f"catch({errors}, where={self.to_string(stream._where)}, do={self.to_string(stream._do)}, replace={self.to_string(stream._replace)}, stop={self.to_string(stream._stop)})"
        )
        return stream.upstream.accept(self)

    def visit_filter_stream(self, stream: "FilterStream") -> str:
        self.operation_reprs.append(f"filter({self.to_string(stream._where)})")
        return stream.upstream.accept(self)

    def visit_flatten_stream(self, stream: "FlattenStream") -> str:
        self.operation_reprs.append(
            f"flatten(concurrency={self.to_string(stream._concurrency)})"
        )
        return stream.upstream.accept(self)

    def visit_do_stream(self, stream: "DoStream") -> str:
        self.operation_reprs.append(
            f"do({self.to_string(stream._effect)}, concurrency={self.to_string(stream._concurrency)}, ordered={self.to_string(stream._ordered)})"
        )
        return stream.upstream.accept(self)

    def visit_group_stream(self, stream: "GroupStream") -> str:
        self.operation_reprs.append(
            f"group(up_to={self.to_string(stream._up_to)}, every={self.to_string(stream._every)}, by={self.to_string(stream._by)})"
        )
        return stream.upstream.accept(self)

    def visit_map_stream(self, stream: "MapStream") -> str:
        self.operation_reprs.append(
            f"map({self.to_string(stream._into)}, concurrency={self.to_string(stream._concurrency)}, ordered={self.to_string(stream._ordered)})"
        )
        return stream.upstream.accept(self)

    def visit_observe_stream(self, stream: "ObserveStream") -> str:
        self.operation_reprs.append(
            f"""observe({self.to_string(stream._subject)}, every={self.to_string(stream._every)})"""
        )
        return stream.upstream.accept(self)

    def visit_skip_stream(self, stream: "SkipStream") -> str:
        self.operation_reprs.append(f"skip(until={self.to_string(stream._until)})")
        return stream.upstream.accept(self)

    def visit_take_stream(self, stream: "TakeStream") -> str:
        self.operation_reprs.append(f"take(until={self.to_string(stream._until)})")
        return stream.upstream.accept(self)

    def visit_throttle_stream(self, stream: "ThrottleStream") -> str:
        self.operation_reprs.append(
            f"throttle({self.to_string(stream._up_to)}, per={self.to_string(stream._per)})"
        )
        return stream.upstream.accept(self)

    def visit_stream(self, stream: "stream") -> str:
        source_stream = f"stream({self.to_string(stream.source)})"
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


class ReprVisitor(ToStringVisitor):
    @staticmethod
    def to_string(o: object) -> str:
        if isinstance(o, _Star):
            return f"star({ReprVisitor.to_string(o.func)})"
        if hasattr(o, "__astarred__"):
            return f"star({ReprVisitor.to_string(getattr(o, '__astarred__'))})"
        return repr(o)


class StrVisitor(ToStringVisitor):
    @staticmethod
    def to_string(o: Any) -> str:
        if isinstance(o, _Star):
            return f"star({StrVisitor.to_string(o.func)})"
        if hasattr(o, "__astarred__"):
            return f"star({StrVisitor.to_string(getattr(o, '__astarred__'))})"
        if type(o) is type and issubclass(o, Exception):
            return o.__name__
        if repr(o).startswith("<"):
            return getattr(o, "__name__", repr(o))
        return repr(o)
