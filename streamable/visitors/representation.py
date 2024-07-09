from typing import List, TypeVar

from streamable.stream import (
    AForeachStream,
    AMapStream,
    CatchStream,
    FilterStream,
    FlattenStream,
    ForeachStream,
    GroupStream,
    MapStream,
    ObserveStream,
    Stream,
    ThrottleStream,
    TruncateStream,
)
from streamable.util import NO_REPLACEMENT
from streamable.visitors import Visitor

T = TypeVar("T")
U = TypeVar("U")


class RepresentationVisitor(Visitor[str]):
    def __init__(self) -> None:
        self.methods_reprs: List[str] = []

    @staticmethod
    def _friendly_repr(o: object) -> str:
        representation = repr(o)
        if representation.startswith("<"):  # default repr
            try:
                representation = getattr(o, "__name__")
            except AttributeError:
                representation = f"{o.__class__.__name__}(...)"
        return representation

    def visit_catch_stream(self, stream: CatchStream[T]) -> str:
        self.methods_reprs.append(
            f"catch({self._friendly_repr(stream._kind)}, when={self._friendly_repr(stream._when)}{(', replacement=' + self._friendly_repr(stream._replacement)) if stream._replacement is not NO_REPLACEMENT else ''}, finally_raise={stream._finally_raise})"
        )
        return stream.upstream.accept(self)

    def visit_filter_stream(self, stream: FilterStream[T]) -> str:
        self.methods_reprs.append(f"filter({self._friendly_repr(stream._keep)})")
        return stream.upstream.accept(self)

    def visit_flatten_stream(self, stream: FlattenStream[T]) -> str:
        self.methods_reprs.append(f"flatten(concurrency={stream._concurrency})")
        return stream.upstream.accept(self)

    def visit_foreach_stream(self, stream: ForeachStream[T]) -> str:
        self.methods_reprs.append(
            f"foreach({self._friendly_repr(stream._effect)}, concurrency={stream._concurrency})"
        )
        return stream.upstream.accept(self)

    def visit_aforeach_stream(self, stream: AForeachStream[T]) -> str:
        self.methods_reprs.append(
            f"aforeach({self._friendly_repr(stream._effect)}, concurrency={stream._concurrency})"
        )
        return stream.upstream.accept(self)

    def visit_group_stream(self, stream: GroupStream[U]) -> str:
        self.methods_reprs.append(
            f"group(size={stream._size}, by={self._friendly_repr(stream._by)}, seconds={stream._seconds})"
        )
        return stream.upstream.accept(self)

    def visit_map_stream(self, stream: MapStream[U, T]) -> str:
        self.methods_reprs.append(
            f"map({self._friendly_repr(stream._transformation)}, concurrency={stream._concurrency})"
        )
        return stream.upstream.accept(self)

    def visit_amap_stream(self, stream: AMapStream[U, T]) -> str:
        self.methods_reprs.append(
            f"amap({self._friendly_repr(stream._transformation)}, concurrency={stream._concurrency})"
        )
        return stream.upstream.accept(self)

    def visit_observe_stream(self, stream: ObserveStream[T]) -> str:
        self.methods_reprs.append(f"""observe("{stream._what}")""")
        return stream.upstream.accept(self)

    def visit_throttle_stream(self, stream: ThrottleStream[T]) -> str:
        self.methods_reprs.append(f"throttle(per_second={stream._per_second})")
        return stream.upstream.accept(self)

    def visit_truncate_stream(self, stream: TruncateStream[T]) -> str:
        self.methods_reprs.append(
            f"truncate(count={stream._count}, when={self._friendly_repr(stream._when)})"
        )
        return stream.upstream.accept(self)

    def visit_stream(self, stream: Stream[T]) -> str:
        methods_block = "".join(
            map(lambda r: f"    .{r}\n", reversed(self.methods_reprs))
        )
        return f"(\n    Stream({self._friendly_repr(stream.source)})\n{methods_block})"
