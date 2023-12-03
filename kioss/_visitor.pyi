from abc import ABC, abstractmethod
from typing import (
    Generic,
    Iterator,
    TypeVar,
)

from kioss import _plan

V = TypeVar("V")
T = TypeVar("T")
U = TypeVar("U")


class APipeVisitor(Generic[V], ABC):
    @abstractmethod
    def visitSourcePipe(self, pipe: _plan.SourcePipe) -> V: ...

    @abstractmethod
    def visitMapPipe(self, pipe: _plan.MapPipe) -> V: ...

    @abstractmethod
    def visitFlattenPipe(self, pipe: _plan.FlattenPipe) -> V: ...

    @abstractmethod
    def visitChainPipe(self, pipe: _plan.ChainPipe) -> V: ...

    @abstractmethod
    def visitFilterPipe(self, pipe: _plan.FilterPipe) -> V: ...

    @abstractmethod
    def visitBatchPipe(self, pipe: _plan.BatchPipe) -> V: ...

    @abstractmethod
    def visitSlowPipe(self, pipe: _plan.SlowPipe) -> V: ...

    @abstractmethod
    def visitCatchPipe(self, pipe: _plan.CatchPipe) -> V: ...

    @abstractmethod
    def visitLogPipe(self, pipe: _plan.LogPipe) -> V: ...

class IteratorGeneratingPipeVisitor(APipeVisitor[Iterator[T]]):
    def visitSourcePipe(self, pipe: _plan.SourcePipe[T]) -> Iterator[T]: ...

    def visitMapPipe(self, pipe: _plan.MapPipe[T]) -> Iterator[T]: ...

    def visitFlattenPipe(self, pipe: _plan.FlattenPipe[T]) -> Iterator[T]: ...

    def visitChainPipe(self, pipe: _plan.ChainPipe[T]) -> Iterator[T]: ...

    def visitFilterPipe(self, pipe: _plan.FilterPipe[T]) -> Iterator[T]: ...

    def visitBatchPipe(self, pipe: _plan.BatchPipe[U]) -> Iterator[T]: ...

    def visitSlowPipe(self, pipe: _plan.SlowPipe[T]) -> Iterator[T]: ...

    def visitCatchPipe(self, pipe: _plan.CatchPipe[T]) -> Iterator[T]: ...

    def visitLogPipe(self, pipe: _plan.LogPipe[T]) -> Iterator[T]: ...
