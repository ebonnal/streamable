from abc import ABC, abstractmethod
from typing import Any

from kioss import _pipe


class AVisitor(ABC):
    @abstractmethod
    def visit_source_pipe(self, pipe: _pipe.SourcePipe) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def visit_map_pipe(self, pipe: _pipe.MapPipe) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def visit_do_pipe(self, pipe: _pipe.DoPipe) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def visit_flatten_pipe(self, pipe: _pipe.FlattenPipe) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def visit_chain_pipe(self, pipe: _pipe.ChainPipe) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def visit_filter_pipe(self, pipe: _pipe.FilterPipe) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def visit_batch_pipe(self, pipe: _pipe.BatchPipe) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def visit_slow_pipe(self, pipe: _pipe.SlowPipe) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def visit_catch_pipe(self, pipe: _pipe.CatchPipe) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def visit_log_pipe(self, pipe: _pipe.LogPipe) -> Any:
        raise NotImplementedError()
