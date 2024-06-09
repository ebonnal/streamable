import textwrap
from typing import cast

from streamable import _util, stream
from streamable.stream import (
    AForeachStream,
    AMapStream,
    CatchStream,
    FilterStream,
    FlattenStream,
    ForeachStream,
    GroupStream,
    LimitStream,
    MapStream,
    ObserveStream,
    SlowStream,
    Stream,
)
from streamable.visitor import Visitor


class ExplanationVisitor(Visitor[str]):
    def __init__(
        self,
        colored: bool = False,
        margin_step: int = 2,
        header: str = "Stream's plan:",
    ) -> None:
        self.colored = colored
        self.header = header
        self.margin_step = margin_step

        self.linking_symbol = "└" + "─" * (self.margin_step - 1) + "•"

        if self.colored:
            self.header = _util.bold(self.header)
        if self.colored:
            self.linking_symbol = _util.colorize_in_grey(self.linking_symbol)

    def _explanation(self, stream: stream.Stream, attributes_repr: str) -> str:
        explanation = self.header

        if self.header:
            explanation += "\n"
            self.header = ""

        name = stream.__class__.__name__
        if self.colored:
            name = _util.colorize_in_red(name)

        stream_repr = f"{name}({attributes_repr})"

        explanation += self.linking_symbol + stream_repr + "\n"

        if stream.upstream is not None:
            explanation += textwrap.indent(
                stream.upstream.accept(self),
                prefix=" " * self.margin_step,
            )

        return explanation

    def visit_stream(self, stream: Stream) -> str:
        return self._explanation(stream, f"source={_util.get_name(stream.source)}")

    def visit_catch_stream(self, stream: CatchStream) -> str:
        return self._explanation(
            stream,
            f"predicate={_util.get_name(stream.predicate)}, raise_at_exhaustion={stream.raise_at_exhaustion}",
        )

    def visit_filter_stream(self, stream: FilterStream) -> str:
        return self._explanation(
            stream, f"predicate={_util.get_name(stream.predicate)}"
        )

    def visit_flatten_stream(self, stream: FlattenStream) -> str:
        return self._explanation(stream, f"concurrency={stream.concurrency}")

    def visit_foreach_stream(self, stream: ForeachStream) -> str:
        return self.visit_map_stream(cast(MapStream, stream))

    def visit_aforeach_stream(self, stream: AForeachStream) -> str:
        return self.visit_map_stream(cast(MapStream, stream))

    def visit_group_stream(self, stream: GroupStream) -> str:
        return self._explanation(
            stream, f"size={stream.size}, seconds={stream.seconds}, by={stream.by}"
        )

    def visit_limit_stream(self, stream: LimitStream) -> str:
        return self._explanation(stream, f"count={stream.count}")

    def visit_map_stream(self, stream: MapStream) -> str:
        return self._explanation(
            stream,
            f"func={_util.get_name(stream.func)}, concurrency={stream.concurrency}",
        )

    def visit_amap_stream(self, stream: AMapStream) -> str:
        return self.visit_map_stream(cast(MapStream, stream))

    def visit_observe_stream(self, stream: ObserveStream) -> str:
        return self._explanation(
            stream, f"what='{stream.what}', colored={stream.colored}"
        )

    def visit_slow_stream(self, stream: SlowStream) -> str:
        return self._explanation(stream, f"frequency={stream.frequency}")
