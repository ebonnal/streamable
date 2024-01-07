from typing import Any

from typing_extensions import override

from streamable import _stream, _util
from streamable._visitors._base import Visitor


class ExplainingVisitor(Visitor[str]):
    HEADER = "Stream's plan:"

    def __init__(
        self, colored: bool = False, initial_margin: int = 0, add_header: bool = True
    ):
        self.colored = colored
        self.current_margin = initial_margin
        self.margin_step = 2
        self.add_header = add_header

    def explanation_line(self, stream: _stream.Stream) -> str:
        stream_str = str(stream)
        end_of_name = stream_str.index("(")
        name = stream_str[:end_of_name]
        args = stream_str[end_of_name:]
        margin = " " * self.current_margin
        if self.add_header:
            linking_symbols = " " * self.margin_step + "•"
        else:
            linking_symbols = "└" + "─" * (self.margin_step - 1) + "•"

        if self.colored:
            linking_symbols = _util.colorize_in_grey(linking_symbols)
            name = _util.colorize_in_red(name)
        return f"{margin}{linking_symbols}{name}{args}\n"

    def explain_stream(self, stream: _stream.Stream) -> str:
        additional_explain_lines = self.explanation_line(stream)
        if self.add_header:
            if self.colored:
                header = _util.bold(ExplainingVisitor.HEADER) + "\n"
            else:
                header = ExplainingVisitor.HEADER + "\n"
            self.add_header = False
        else:
            header = ""
        self.current_margin += self.margin_step
        upstream = stream.upstream()
        if upstream is not None:
            upstream_repr = upstream._accept(self)
        else:
            upstream_repr = ""
        return f"{header}{additional_explain_lines}{upstream_repr}"

    @override
    def visit_batch_stream(self, stream: _stream.BatchStream) -> Any:
        return self.explain_stream(stream)

    @override
    def visit_catch_stream(self, stream: _stream.CatchStream) -> Any:
        return self.explain_stream(stream)

    @override
    def visit_chain_stream(self, stream: _stream.ChainStream) -> Any:
        additional_explain_lines = self.explanation_line(stream)
        self.current_margin += self.margin_step
        chained_streams_repr = "".join(
            map(
                lambda stream: stream._accept(
                    ExplainingVisitor(
                        self.colored, self.current_margin, add_header=False
                    )
                ),
                stream.others,
            )
        )
        upstream_repr = stream.upstream()._accept(self)
        return f"{additional_explain_lines}{chained_streams_repr}{upstream_repr}"

    @override
    def visit_do_stream(self, stream: _stream.DoStream) -> Any:
        return self.explain_stream(stream)

    @override
    def visit_filter_stream(self, stream: _stream.FilterStream) -> Any:
        return self.explain_stream(stream)

    @override
    def visit_flatten_stream(self, stream: _stream.FlattenStream) -> Any:
        return self.explain_stream(stream)

    @override
    def visit_observe_stream(self, stream: _stream.ObserveStream) -> Any:
        return self.explain_stream(stream)

    @override
    def visit_map_stream(self, stream: _stream.MapStream) -> Any:
        return self.explain_stream(stream)

    def visit_slow_stream(self, stream: _stream.SlowStream) -> Any:
        return self.explain_stream(stream)

    @override
    def visit_stream(self, stream: _stream.Stream) -> Any:
        return self.explain_stream(stream)
