from abc import ABC, abstractmethod
from typing import Any

from kioss import _pipe

class AVisitor(ABC):
    @abstractmethod
    def visitSourcePipe(self, pipe: _pipe.SourcePipe) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def visitMapPipe(self, pipe: _pipe.MapPipe) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def visitFlattenPipe(self, pipe: _pipe.FlattenPipe) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def visitChainPipe(self, pipe: _pipe.ChainPipe) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def visitFilterPipe(self, pipe: _pipe.FilterPipe) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def visitBatchPipe(self, pipe: _pipe.BatchPipe) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def visitSlowPipe(self, pipe: _pipe.SlowPipe) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def visitCatchPipe(self, pipe: _pipe.CatchPipe) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def visitLogPipe(self, pipe: _pipe.LogPipe) -> Any:
        raise NotImplementedError()
