import textwrap

from streamable import _util, stream
from streamable.visitor import Visitor


class ExplanationVisitor(Visitor[str]):
    def __init__(
        self,
        colored: bool = False,
        margin_step: int = 2,
        header: str = "Stream's plan:",
    ):
        self.colored = colored
        self.header = header
        self.margin_step = margin_step

        self.linking_symbol = "└" + "─" * (self.margin_step - 1) + "•"

        if self.colored:
            self.header = _util.bold(self.header)
        if self.colored:
            self.linking_symbol = _util.colorize_in_grey(self.linking_symbol)

    def visit_any(self, stream: stream.Stream) -> str:
        explanation = self.header

        if self.header:
            explanation += "\n"
            self.header = ""

        stream_repr = repr(stream)
        if self.colored:
            name, rest = stream_repr.split("(", maxsplit=1)
            stream_repr = _util.colorize_in_red(name) + "(" + rest

        explanation += self.linking_symbol + stream_repr + "\n"

        upstream = stream.upstream
        if upstream is not None:
            explanation += textwrap.indent(
                upstream.accept(self),
                prefix=" " * self.margin_step,
            )

        return explanation
