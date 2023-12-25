from typing import Any

from streamable import _stream, _util
from streamable._visit._base import Visitor


class ExplainingVisitor(Visitor[str]):
    HEADER = "Stream's plan:"

    def __init__(
        self, colored: bool = False, initial_margin: int = 0, add_header: bool = True
    ):
        self.colored = colored
        self.current_margin = initial_margin
        self.margin_step = 2
        self.add_header = add_header

    def additional_explain_lines(self, name: str, descr: str) -> str:
        margin = " " * self.current_margin
        if self.add_header:
            linking_symbols = " " * self.margin_step + "•"
        else:
            linking_symbols = "└" + "─" * (self.margin_step - 1) + "•"

        if self.colored:
            linking_symbols = _util.colorize_in_grey(linking_symbols)
            name = _util.colorize_in_red(name)
        return f"{margin}{linking_symbols}{name}({descr})\n"

    def visit_any_stream(self, stream: _stream.Stream, name: str, descr: str) -> str:
        additional_explain_lines = self.additional_explain_lines(name, descr)
        if self.add_header:
            if self.colored:
                header = _util.bold(ExplainingVisitor.HEADER) + "\n"
            else:
                header = ExplainingVisitor.HEADER + "\n"
            self.add_header = False
        else:
            header = ""
        self.current_margin += self.margin_step
        if stream.upstream is not None:
            upstream_repr = stream.upstream._accept(self)
        else:
            upstream_repr = ""
        return f"{header}{additional_explain_lines}{upstream_repr}"

    def visit_chain_stream(self, stream: _stream.ChainStream) -> Any:
        name = "Chain"
        descr = f"{len(stream.others)+1} streams"
        additional_explain_lines = self.additional_explain_lines(name, descr)
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
        upstream_repr = stream.upstream._accept(self)
        return f"{additional_explain_lines}{chained_streams_repr}{upstream_repr}"

    def visit_source_stream(self, stream: _stream.Stream) -> Any:
        name = "Source"
        descr = f"from: {stream.source}"
        return self.visit_any_stream(stream, name, descr)

    def visit_map_stream(self, stream: _stream.MapStream) -> Any:
        name = "Map"
        descr = f"function {stream.func}, using {stream.n_threads} thread{'s' if stream.n_threads > 1 else ''}"
        return self.visit_any_stream(stream, name, descr)

    def visit_do_stream(self, stream: _stream.DoStream) -> Any:
        name = "Do"
        descr = f"side effects by applying a function {stream.func}, using {stream.n_threads} thread{'s' if stream.n_threads > 1 else ''}"
        return self.visit_any_stream(stream, name, descr)

    def visit_flatten_stream(self, stream: _stream.FlattenStream) -> Any:
        name = "Flatten"
        descr = f"using {stream.n_threads} thread{'s' if stream.n_threads > 1 else ''}"
        return self.visit_any_stream(stream, name, descr)

    def visit_filter_stream(self, stream: _stream.FilterStream) -> Any:
        name = "Filter"
        descr = f"using predicate function {stream.predicate}"
        return self.visit_any_stream(stream, name, descr)

    def visit_batch_stream(self, stream: _stream.BatchStream) -> Any:
        name = "Batch"
        descr = f"elements by groups of {stream.size} element{'s' if stream.size > 1 else ''}, or over a seconds of {stream.seconds} second{'s' if stream.seconds > 1 else ''}"
        return self.visit_any_stream(stream, name, descr)

    def visit_slow_stream(self, stream: _stream.SlowStream) -> Any:
        name = "Slow"
        descr = f"at a maximum frequency of {stream.frequency} element{'s' if stream.frequency > 1 else ''} per second"
        return self.visit_any_stream(stream, name, descr)

    def visit_catch_stream(self, stream: _stream.CatchStream) -> Any:
        name = "Catch"
        descr = f"exception instances of class in [{', '.join(map(lambda class_: class_.__name__, stream.classes))}]{', with an additional `when` condition' if stream.when is not None else ''}"
        return self.visit_any_stream(stream, name, descr)

    def visit_observe_stream(self, stream: _stream.ObserveStream) -> Any:
        name = "Observe"
        descr = f"the evolution of the iteration over '{stream.what}'"
        return self.visit_any_stream(stream, name, descr)
