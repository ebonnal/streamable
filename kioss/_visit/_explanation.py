from typing import Any

from kioss import _pipe, _util
from kioss._visit._base import Visitor


class ExplainingVisitor(Visitor[str]):
    HEADER = "Pipe's plan:"

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

    def visit_any_pipe(self, pipe: _pipe.Pipe, name: str, descr: str) -> str:
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
        if pipe.upstream is not None:
            upstream_repr = pipe.upstream._accept(self)
        else:
            upstream_repr = ""
        return f"{header}{additional_explain_lines}{upstream_repr}"

    def visit_chain_pipe(self, pipe: _pipe.ChainPipe) -> Any:
        name = "Chain"
        descr = f"{len(pipe.others)+1} pipes"
        additional_explain_lines = self.additional_explain_lines(name, descr)
        self.current_margin += self.margin_step
        chained_pipes_repr = "".join(
            map(
                lambda pipe: pipe._accept(
                    ExplainingVisitor(
                        self.colored, self.current_margin, add_header=False
                    )
                ),
                pipe.others,
            )
        )
        upstream_repr = pipe.upstream._accept(self)
        return f"{additional_explain_lines}{chained_pipes_repr}{upstream_repr}"

    def visit_source_pipe(self, pipe: _pipe.Pipe) -> Any:
        name = "Source"
        descr = f"from: {pipe.source}"
        return self.visit_any_pipe(pipe, name, descr)

    def visit_map_pipe(self, pipe: _pipe.MapPipe) -> Any:
        name = "Map"
        descr = f"function {pipe.func}, using {pipe.n_threads} thread{'s' if pipe.n_threads > 1 else ''}"
        return self.visit_any_pipe(pipe, name, descr)

    def visit_do_pipe(self, pipe: _pipe.DoPipe) -> Any:
        name = "Do"
        descr = f"side effects by applying a function {pipe.func}, using {pipe.n_threads} thread{'s' if pipe.n_threads > 1 else ''}"
        return self.visit_any_pipe(pipe, name, descr)

    def visit_flatten_pipe(self, pipe: _pipe.FlattenPipe) -> Any:
        name = "Flatten"
        descr = f"using {pipe.n_threads} thread{'s' if pipe.n_threads > 1 else ''}"
        return self.visit_any_pipe(pipe, name, descr)

    def visit_filter_pipe(self, pipe: _pipe.FilterPipe) -> Any:
        name = "Filter"
        descr = f"using predicate function {pipe.predicate}"
        return self.visit_any_pipe(pipe, name, descr)

    def visit_batch_pipe(self, pipe: _pipe.BatchPipe) -> Any:
        name = "Batch"
        descr = f"elements by groups of {pipe.size} element{'s' if pipe.size > 1 else ''}, or over a period of {pipe.period} second{'s' if pipe.period > 1 else ''}"
        return self.visit_any_pipe(pipe, name, descr)

    def visit_slow_pipe(self, pipe: _pipe.SlowPipe) -> Any:
        name = "Slow"
        descr = f"at a maximum frequency of {pipe.freq} element{'s' if pipe.freq > 1 else ''} per second"
        return self.visit_any_pipe(pipe, name, descr)

    def visit_catch_pipe(self, pipe: _pipe.CatchPipe) -> Any:
        name = "Catch"
        descr = f"exception instances of class in [{', '.join(map(lambda class_: class_.__name__, pipe.classes))}]{', with an additional `when` condition' if pipe.when is not None else ''}"
        return self.visit_any_pipe(pipe, name, descr)

    def visit_log_pipe(self, pipe: _pipe.LogPipe) -> Any:
        name = "Log"
        descr = f"the evolution of the iteration over '{pipe.what}'"
        return self.visit_any_pipe(pipe, name, descr)
