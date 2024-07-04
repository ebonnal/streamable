import textwrap
from typing import Optional

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
    Stream,
    ThrottleStream,
    TruncateStream,
)
from streamable.visitor import Visitor


def friendly_string(o: object) -> str:
    if o is None:
        return "None"
    try:
        return o.__name__  # type: ignore
    except AttributeError:
        if len(repr(o)) < 16:
            return repr(o)
        return o.__class__.__name__ + "(...)"


class ExplanationVisitor(Visitor[str]):
    def __init__(
        self,
        margin_step: int = 2,
    ) -> None:
        self.margin_step = margin_step
        self.linking_symbol = "└" + "─" * (self.margin_step - 1)

    def _explanation(self, upstream: Optional[stream.Stream], stream_repr: str) -> str:

        if upstream is not None:
            explanation = self.linking_symbol + "•" + stream_repr + "\n"
            explanation += textwrap.indent(
                upstream.accept(self),
                prefix=" " * self.margin_step,
            )
        else:
            explanation = self.linking_symbol + "┤" + stream_repr + "\n"

        return explanation

    def visit_stream(self, stream: Stream) -> str:
        return self._explanation(
            stream.upstream, f"Stream({friendly_string(stream.source)})"
        )

    def visit_catch_stream(self, stream: CatchStream) -> str:
        return self._explanation(
            stream.upstream,
            f"catch({friendly_string(stream.kind)}, finally_raise={stream.finally_raise})",
        )

    def visit_filter_stream(self, stream: FilterStream) -> str:
        return self._explanation(
            stream.upstream, f"filter({friendly_string(stream.keep)})"
        )

    def visit_flatten_stream(self, stream: FlattenStream) -> str:
        return self._explanation(
            stream.upstream, f"flatten(concurrency={stream.concurrency})"
        )

    def visit_foreach_stream(self, stream: ForeachStream) -> str:
        return self._explanation(
            stream.upstream,
            f"foreach({friendly_string(stream.effect)}, concurrency={stream.concurrency})",
        )

    def visit_aforeach_stream(self, stream: AForeachStream) -> str:
        return self._explanation(
            stream.upstream,
            f"aforeach({friendly_string(stream.effect)}, concurrency={stream.concurrency})",
        )

    def visit_group_stream(self, stream: GroupStream) -> str:
        return self._explanation(
            stream.upstream,
            f"group(size={stream.size}, by={friendly_string(stream.by)}, seconds={stream.seconds})",
        )

    def visit_truncate_stream(self, stream: TruncateStream) -> str:
        return self._explanation(
            stream.upstream,
            f"truncate(count={stream.count}, when={friendly_string(stream.when)})",
        )

    def visit_map_stream(self, stream: MapStream) -> str:
        return self._explanation(
            stream.upstream,
            f"map({friendly_string(stream.transformation)}, concurrency={stream.concurrency})",
        )

    def visit_amap_stream(self, stream: AMapStream) -> str:
        return self._explanation(
            stream.upstream,
            f"amap({friendly_string(stream.transformation)}, concurrency={stream.concurrency})",
        )

    def visit_observe_stream(self, stream: ObserveStream) -> str:
        return self._explanation(stream.upstream, f"""observe("{stream.what}")""")

    def visit_throttle_stream(self, stream: ThrottleStream) -> str:
        return self._explanation(
            stream.upstream, f"throttle(per_second={stream.per_second})"
        )
