import itertools
from abc import ABC, abstractmethod
from typing import (
    Generic,
    Iterator,
    List,
    TypeVar,
)

from kioss import _exec, _concurrent_exec, _pipe, _util, _visitor

V = TypeVar("V")
T = TypeVar("T")
U = TypeVar("U")

class IteratorGeneratingVisitor(_visitor.AVisitor):

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
            return map(pipe.func, iter(pipe.upstream))
        else:
            return _concurrent_exec.ThreadedMappingIteratorWrapper(
                iter(pipe.upstream), pipe.func, n_workers=pipe.n_threads
            ) 

    def visitFlattenPipe(self, pipe: _pipe.FlattenPipe[T]) -> Iterator[T]:
        upstream_pipe: _pipe.APipe[Iterator[T]] = pipe.upstream
        if pipe.n_threads == 1:
            return _exec.FlatteningIteratorWrapper(iter(upstream_pipe))
        else:
            return _concurrent_exec.ThreadedFlatteningIteratorWrapper(
                iter(pipe.upstream), n_workers=pipe.n_threads
            )

    def visitChainPipe(self, pipe: _pipe.ChainPipe[T]) -> Iterator[T]:
        return itertools.chain(iter(pipe.upstream), *list(map(iter, pipe.others)))

    def visitFilterPipe(self, pipe: _pipe.FilterPipe[T]) -> Iterator[T]:
        return filter(pipe.predicate, iter(pipe.upstream))

    def visitBatchPipe(self, pipe: _pipe.BatchPipe[U]) -> Iterator[List[U]]:
        return _exec.BatchingIteratorWrapper(
            iter(pipe.upstream), pipe.size, pipe.period
        )

    def visitSlowPipe(self, pipe: _pipe.SlowPipe[T]) -> Iterator[T]:
        return _exec.SlowingIteratorWrapper(iter(pipe.upstream), pipe.freq)

    def visitCatchPipe(self, pipe: _pipe.CatchPipe[T]) -> Iterator[T]:
        return _exec.CatchingIteratorWrapper(
            iter(pipe.upstream), *pipe.classes, when=pipe.when
        )

    def visitLogPipe(self, pipe: _pipe.LogPipe[T]) -> Iterator[T]:
        return _exec.LoggingIteratorWrapper(iter(pipe.upstream), pipe.what)
