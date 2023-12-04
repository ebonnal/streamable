import itertools
from abc import ABC, abstractmethod
from typing import (
    Any,
    Iterator,
    List,
    TypeVar,
)

from kioss import _exec, _concurrent_exec, _pipe, _util

T = TypeVar("T")

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

class ExplainingVisitor(AVisitor):
    def __init__(self, n_initial_left_margin_spaces: int = 0):
        self.current_margin = n_initial_left_margin_spaces
        self.margin_step = 2
        self.add_header = True
    
    def visitAnyPipe(self, pipe: _pipe.APipe) -> str:
        if self.add_header:    
            header = "\033[1mPipe's plan\033[0m:\n"
            self.add_header = False
        else:
            header = ''
        name, descr = str(pipe).split('(')
        colored_pipe_str = f"\033[91m{name}\033[0m({descr}"
        additional_repr_lines = f"\033[92m+{'-'*self.current_margin}\033[0m {colored_pipe_str}\n"
        self.current_margin += self.margin_step
        if pipe.upstream is not None:
            upstream_repr = pipe.upstream._accept(self)
        else:
            upstream_repr = ''
        return header + additional_repr_lines + upstream_repr

    def visitSourcePipe(self, pipe: _pipe.SourcePipe) -> Any:
        return self.visitAnyPipe(pipe)

    def visitMapPipe(self, pipe: _pipe.MapPipe) -> Any:
        return self.visitAnyPipe(pipe)

    def visitFlattenPipe(self, pipe: _pipe.FlattenPipe) -> Any:
        return self.visitAnyPipe(pipe)

    def visitChainPipe(self, pipe: _pipe.ChainPipe) -> Any:
        return self.visitAnyPipe(pipe)

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

class IteratorGeneratingVisitor(AVisitor):
    def visitSourcePipe(self, pipe: _pipe.SourcePipe[T]) -> Iterator[T]:
        iterator = pipe.source()
        try:
            # duck-type checks that the object returned by the source is an iterator
            _util.duck_check_type_is_iterator(iterator)
        except TypeError as e:
            raise TypeError(
                f"source must be a callable returning an iterator (implements __iter__ and __next__ methods), but the object resulting from a call to source() was not an iterator: got '{iterator}' of type {type(iterator)}."
            ) from e
        return iterator

    def visitMapPipe(self, pipe: _pipe.MapPipe[T]) -> Iterator[T]:
        if pipe.n_threads == 1:
            return map(pipe.func, pipe.upstream._accept(self))
        else:
            return _concurrent_exec.ThreadedMappingIteratorWrapper(
                pipe.upstream._accept(self), pipe.func, n_workers=pipe.n_threads
            ) 

    def visitFlattenPipe(self, pipe: _pipe.FlattenPipe[T]) -> Iterator[T]:
        if pipe.n_threads == 1:
            return _exec.FlatteningIteratorWrapper(pipe.upstream._accept(self))
        else:
            return _concurrent_exec.ThreadedFlatteningIteratorWrapper(
                pipe.upstream._accept(self), n_workers=pipe.n_threads
            )

    def visitChainPipe(self, pipe: _pipe.ChainPipe[T]) -> Iterator[T]:
        return itertools.chain(pipe.upstream._accept(self), *list(map(iter, pipe.others)))

    def visitFilterPipe(self, pipe: _pipe.FilterPipe[T]) -> Iterator[T]:
        return filter(pipe.predicate, pipe.upstream._accept(self))

    def visitBatchPipe(self, pipe: _pipe.BatchPipe[T]) -> Iterator[List[T]]:
        return _exec.BatchingIteratorWrapper(
            pipe.upstream._accept(self), pipe.size, pipe.period
        )

    def visitSlowPipe(self, pipe: _pipe.SlowPipe[T]) -> Iterator[T]:
        return _exec.SlowingIteratorWrapper(pipe.upstream._accept(self), pipe.freq)

    def visitCatchPipe(self, pipe: _pipe.CatchPipe[T]) -> Iterator[T]:
        return _exec.CatchingIteratorWrapper(
            pipe.upstream._accept(self), *pipe.classes, when=pipe.when
        )

    def visitLogPipe(self, pipe: _pipe.LogPipe[T]) -> Iterator[T]:
        return _exec.LoggingIteratorWrapper(pipe.upstream._accept(self), pipe.what)
