import textwrap
from typing import cast

from streamable import stream
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
    SlowStream,
    Stream,
    TruncateStream,
)
from streamable.visitor import Visitor


def get_name(o: object) -> str:
    if o is None:
        return "None"
    try:
        return o.__name__  # type: ignore
    except AttributeError:
        return o.__class__.__name__ + "(...)"


class ExplanationVisitor(Visitor[str]):
    def __init__(
        self,
        margin_step: int = 2,
    ) -> None:
        self.margin_step = margin_step
        self.linking_symbol = "└" + "─" * (self.margin_step - 1) + "•"

    def _explanation(self, stream: stream.Stream, attributes_repr: str) -> str:
        name = stream.__class__.__name__
        stream_repr = f"{name}({attributes_repr})"
        explanation = self.linking_symbol + stream_repr + "\n"

        if stream.upstream is not None:
            explanation += textwrap.indent(
                stream.upstream.accept(self),
                prefix=" " * self.margin_step,
            )

        return explanation

    def visit_stream(self, stream: Stream) -> str:
        return self._explanation(stream, f"source={get_name(stream.source)}")

    def visit_catch_stream(self, stream: CatchStream) -> str:
        return self._explanation(
            stream,
            f"kind={get_name(stream.kind)}, finally_raise={stream.finally_raise}",
        )

    def visit_filter_stream(self, stream: FilterStream) -> str:
        return self._explanation(stream, f"keep={get_name(stream.keep)}")

    def visit_flatten_stream(self, stream: FlattenStream) -> str:
        return self._explanation(stream, f"concurrency={stream.concurrency}")

    def visit_foreach_stream(self, stream: ForeachStream) -> str:
        return self._explanation(
            stream,
            f"effect={get_name(stream.effect)}, concurrency={stream.concurrency}",
        )

    def visit_aforeach_stream(self, stream: AForeachStream) -> str:
        return self.visit_foreach_stream(cast(ForeachStream, stream))

    def visit_group_stream(self, stream: GroupStream) -> str:
        return self._explanation(
            stream,
            f"size={stream.size}, seconds={stream.seconds}, by={get_name(stream.by)}",
        )

    def visit_truncate_stream(self, stream: TruncateStream) -> str:
        return self._explanation(
            stream, f"count={stream.count}, when={get_name(stream.when)}"
        )

    def visit_map_stream(self, stream: MapStream) -> str:
        return self._explanation(
            stream,
            f"transformation={get_name(stream.transformation)}, concurrency={stream.concurrency}",
        )

    def visit_amap_stream(self, stream: AMapStream) -> str:
        return self.visit_map_stream(cast(MapStream, stream))

    def visit_observe_stream(self, stream: ObserveStream) -> str:
        return self._explanation(stream, f"what='{stream.what}'")

    def visit_slow_stream(self, stream: SlowStream) -> str:
        return self._explanation(stream, f"frequency={stream.frequency}")
