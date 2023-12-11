from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from kioss import _pipe

V = TypeVar("V")


class Visitor(Generic[V], ABC):
    @abstractmethod
    def visit_source_pipe(self, pipe: _pipe.Pipe) -> V:
        ...

    @abstractmethod
    def visit_map_pipe(self, pipe: _pipe.MapPipe) -> V:
        ...

    @abstractmethod
    def visit_do_pipe(self, pipe: _pipe.DoPipe) -> V:
        ...

    @abstractmethod
    def visit_flatten_pipe(self, pipe: _pipe.FlattenPipe) -> V:
        ...

    @abstractmethod
    def visit_chain_pipe(self, pipe: _pipe.ChainPipe) -> V:
        ...

    @abstractmethod
    def visit_filter_pipe(self, pipe: _pipe.FilterPipe) -> V:
        ...

    @abstractmethod
    def visit_batch_pipe(self, pipe: _pipe.BatchPipe) -> V:
        ...

    @abstractmethod
    def visit_slow_pipe(self, pipe: _pipe.SlowPipe) -> V:
        ...

    @abstractmethod
    def visit_catch_pipe(self, pipe: _pipe.CatchPipe) -> V:
        ...

    @abstractmethod
    def visit_log_pipe(self, pipe: _pipe.LogPipe) -> V:
        ...
