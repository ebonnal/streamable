from typing import Any

from kioss import _pipe, _util
from kioss._visitors._base import AVisitor

class ExplainingVisitor(AVisitor):
    HEADER = "Pipe's plan:"
    def __init__(self, initial_margin: int = 0, add_header: bool = True):
        self.current_margin = initial_margin
        self.margin_step = 4
        self.add_header = add_header

    def additional_explain_lines(self, pipe: _pipe.APipe) -> str:
        name, descr = str(pipe).split('(')
        return f"{' '*self.current_margin}{_util.colorize_in_grey('└' + '─'*(self.margin_step - 1))}{_util.colorize_in_red(name)}({descr}\n"

    def visitAnyPipe(self, pipe: _pipe.APipe) -> str:
        if self.add_header:    
            header = _util.bold(ExplainingVisitor.HEADER) + '\n'
            self.add_header = False
        else:
            header = ''
        additional_explain_lines = self.additional_explain_lines(pipe)
        self.current_margin += self.margin_step
        if pipe.upstream is not None:
            upstream_repr = pipe.upstream._accept(self)
        else:
            upstream_repr = ''
        return f"{header}{additional_explain_lines}{upstream_repr}"

    def visitSourcePipe(self, pipe: _pipe.SourcePipe) -> Any:
        return self.visitAnyPipe(pipe)

    def visitMapPipe(self, pipe: _pipe.MapPipe) -> Any:
        return self.visitAnyPipe(pipe)

    def visitFlattenPipe(self, pipe: _pipe.FlattenPipe) -> Any:
        return self.visitAnyPipe(pipe)

    def visitChainPipe(self, pipe: _pipe.ChainPipe) -> Any:
        additional_explain_lines = self.additional_explain_lines(pipe)
        self.current_margin += self.margin_step
        return f"{additional_explain_lines}{''.join(map(lambda pipe: pipe._accept(ExplainingVisitor(self.current_margin, add_header=False)), pipe.others))}{self.visitAnyPipe(pipe.upstream)}"

    def visitFilterPipe(self, pipe: _pipe.FilterPipe) -> Any:
        return self.visitAnyPipe(pipe)

    def visitBatchPipe(self, pipe: _pipe.BatchPipe) -> Any:
        return self.visitAnyPipe(pipe)

    def visitSlowPipe(self, pipe: _pipe.SlowPipe) -> Any:
        return self.visitAnyPipe(pipe)

    def visitCatchPipe(self, pipe: _pipe.CatchPipe) -> Any:
        return self.visitAnyPipe(pipe)

    def visitLogPipe(self, pipe: _pipe.LogPipe) -> Any:
        return self.visitAnyPipe(pipe)
