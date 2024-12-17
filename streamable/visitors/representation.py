from abc import ABC, abstractmethod
from typing import List, TypeVar

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
from streamable.util.constants import NO_REPLACEMENT
from streamable.util.functiontools import _Star
from streamable.visitors import Visitor

T = TypeVar("T")
U = TypeVar("U")


class ToStringVisitor(Visitor[str], ABC):
    def __init__(self) -> None:
        self.methods_reprs: List[str] = []

    @staticmethod
    @abstractmethod
    def to_string(o: object) -> str: ...

    def visit_catch_stream(self, stream: CatchStream[T]) -> str:
        replacement = (
            f", replacement={self.to_string(stream._replacement)}"
            if stream._replacement is not NO_REPLACEMENT
            else ""
        )
        self.methods_reprs.append(
            f"catch({self.to_string(stream._kind)}, when={self.to_string(stream._when)}{replacement}, finally_raise={self.to_string(stream._finally_raise)})"
        )
        return stream.upstream.accept(self)

    def visit_distinct_stream(self, stream: DistinctStream[T]) -> str:
        self.methods_reprs.append(
            f"distinct({self.to_string(stream._by)}, consecutive_only={self.to_string(stream._consecutive_only)})"
        )
        return stream.upstream.accept(self)

    def visit_filter_stream(self, stream: FilterStream[T]) -> str:
        self.methods_reprs.append(f"filter({self.to_string(stream._when)})")
        return stream.upstream.accept(self)

    def visit_flatten_stream(self, stream: FlattenStream[T]) -> str:
        self.methods_reprs.append(
            f"flatten(concurrency={self.to_string(stream._concurrency)})"
        )
        return stream.upstream.accept(self)

    def visit_foreach_stream(self, stream: ForeachStream[T]) -> str:
        via = f", via={self.to_string(stream._via)}" if stream._concurrency > 1 else ""
        self.methods_reprs.append(
            f"foreach({self.to_string(stream._effect)}, concurrency={self.to_string(stream._concurrency)}, ordered={self.to_string(stream._ordered)}{via})"
        )
        return stream.upstream.accept(self)

    def visit_aforeach_stream(self, stream: AForeachStream[T]) -> str:
        self.methods_reprs.append(
            f"aforeach({self.to_string(stream._effect)}, concurrency={self.to_string(stream._concurrency)}, ordered={self.to_string(stream._ordered)})"
        )
        return stream.upstream.accept(self)

    def visit_group_stream(self, stream: GroupStream[U]) -> str:
        self.methods_reprs.append(
            f"group(size={self.to_string(stream._size)}, by={self.to_string(stream._by)}, interval={self.to_string(stream._interval)})"
        )
        return stream.upstream.accept(self)

    def visit_groupby_stream(self, stream: GroupbyStream[U, T]) -> str:
        self.methods_reprs.append(
            f"groupby({self.to_string(stream._by)}, size={self.to_string(stream._size)}, interval={self.to_string(stream._interval)})"
        )
        return stream.upstream.accept(self)

    def visit_map_stream(self, stream: MapStream[U, T]) -> str:
        via = f", via={self.to_string(stream._via)}" if stream._concurrency > 1 else ""
        self.methods_reprs.append(
            f"map({self.to_string(stream._transformation)}, concurrency={self.to_string(stream._concurrency)}, ordered={self.to_string(stream._ordered)}{via})"
        )
        return stream.upstream.accept(self)

    def visit_amap_stream(self, stream: AMapStream[U, T]) -> str:
        self.methods_reprs.append(
            f"amap({self.to_string(stream._transformation)}, concurrency={self.to_string(stream._concurrency)}, ordered={self.to_string(stream._ordered)})"
        )
        return stream.upstream.accept(self)

    def visit_observe_stream(self, stream: ObserveStream[T]) -> str:
        self.methods_reprs.append(f"""observe({self.to_string(stream._what)})""")
        return stream.upstream.accept(self)

    def visit_skip_stream(self, stream: SkipStream[T]) -> str:
        self.methods_reprs.append(f"skip({self.to_string(stream._count)})")
        return stream.upstream.accept(self)

    def visit_throttle_stream(self, stream: ThrottleStream[T]) -> str:
        self.methods_reprs.append(
            f"throttle(per_second={self.to_string(stream._per_second)}, per_minute={self.to_string(stream._per_minute)}, per_hour={self.to_string(stream._per_hour)}, interval={self.to_string(stream._interval)})"
        )
        return stream.upstream.accept(self)

    def visit_truncate_stream(self, stream: TruncateStream[T]) -> str:
        self.methods_reprs.append(
            f"truncate(count={self.to_string(stream._count)}, when={self.to_string(stream._when)})"
        )
        return stream.upstream.accept(self)

    def visit_stream(self, stream: Stream[T]) -> str:
        methods_block = "".join(
            map(lambda r: f"    .{r}\n", reversed(self.methods_reprs))
        )
        return f"(\n    Stream({self.to_string(stream.source)})\n{methods_block})"


class ReprVisitor(ToStringVisitor):
    @staticmethod
    def to_string(o: object) -> str:
        if isinstance(o, _Star):
            return f"star({ReprVisitor.to_string(o.func)})"
        return repr(o)


class StrVisitor(ToStringVisitor):
    @staticmethod
    def to_string(o: object) -> str:
        if isinstance(o, _Star):
            return f"star({StrVisitor.to_string(o.func)})"
        if repr(o).startswith("<"):
            try:
                return getattr(o, "__name__")
            except AttributeError:
                return f"{o.__class__.__name__}(...)"
        return repr(o)
