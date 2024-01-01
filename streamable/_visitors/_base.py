from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from streamable import _stream

V = TypeVar("V")


class Visitor(Generic[V], ABC):
    @abstractmethod
    def visit_source_stream(self, stream: _stream.Stream) -> V:
        ...

    @abstractmethod
    def visit_map_stream(self, stream: _stream.MapStream) -> V:
        ...

    @abstractmethod
    def visit_do_stream(self, stream: _stream.DoStream) -> V:
        ...

    @abstractmethod
    def visit_flatten_stream(self, stream: _stream.FlattenStream) -> V:
        ...

    @abstractmethod
    def visit_chain_stream(self, stream: _stream.ChainStream) -> V:
        ...

    @abstractmethod
    def visit_filter_stream(self, stream: _stream.FilterStream) -> V:
        ...

    @abstractmethod
    def visit_batch_stream(self, stream: _stream.BatchStream) -> V:
        ...

    @abstractmethod
    def visit_slow_stream(self, stream: _stream.SlowStream) -> V:
        ...

    @abstractmethod
    def visit_catch_stream(self, stream: _stream.CatchStream) -> V:
        ...

    @abstractmethod
    def visit_observe_stream(self, stream: _stream.ObserveStream) -> V:
        ...
