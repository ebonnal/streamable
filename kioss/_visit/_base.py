from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from kioss import _pipe

V = TypeVar("V")


class AVisitor(Generic[V], ABC):
    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def visit_source_pipe(self, pipe: _pipe.SourcePipe) -> V:
        raise NotImplementedError()

    @abstractmethod
    def visit_map_pipe(self, pipe: _pipe.MapPipe) -> V:
        raise NotImplementedError()

    @abstractmethod
    def visit_do_pipe(self, pipe: _pipe.DoPipe) -> V:
        raise NotImplementedError()

    @abstractmethod
    def visit_flatten_pipe(self, pipe: _pipe.FlattenPipe) -> V:
        raise NotImplementedError()

    @abstractmethod
    def visit_chain_pipe(self, pipe: _pipe.ChainPipe) -> V:
        raise NotImplementedError()

    @abstractmethod
    def visit_filter_pipe(self, pipe: _pipe.FilterPipe) -> V:
        raise NotImplementedError()

    @abstractmethod
    def visit_batch_pipe(self, pipe: _pipe.BatchPipe) -> V:
        raise NotImplementedError()

    @abstractmethod
    def visit_slow_pipe(self, pipe: _pipe.SlowPipe) -> V:
        raise NotImplementedError()

    @abstractmethod
    def visit_catch_pipe(self, pipe: _pipe.CatchPipe) -> V:
        raise NotImplementedError()

    @abstractmethod
    def visit_log_pipe(self, pipe: _pipe.LogPipe) -> V:
        raise NotImplementedError()
