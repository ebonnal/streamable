import itertools
from typing import (
    Iterator,
    List,
    TypeVar,
)

from kioss import _pipe, _util
from kioss._execution import _concurrency, _core
from kioss._visit._base import AVisitor

T = TypeVar("T")


class IteratorProducingVisitor(AVisitor):
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
        func = _util.map_exception(pipe.func, source=StopIteration, target=RuntimeError)
        if pipe.n_threads == 1:
            return map(func, pipe.upstream._accept(self))
        else:
            return _concurrency.ThreadedMappingIteratorWrapper(
                pipe.upstream._accept(self), func, n_workers=pipe.n_threads
            )

    def visitDoPipe(self, pipe: _pipe.DoPipe[T]) -> Iterator[T]:
        return self.visitMapPipe(
            _pipe.MapPipe(pipe.upstream, _util.sidify(pipe.func), pipe.n_threads)
        )

    def visitFlattenPipe(self, pipe: _pipe.FlattenPipe[T]) -> Iterator[T]:
        if pipe.n_threads == 1:
            return _core.FlatteningIteratorWrapper(pipe.upstream._accept(self))
        else:
            return _concurrency.ThreadedFlatteningIteratorWrapper(
                pipe.upstream._accept(self), n_workers=pipe.n_threads
            )

    def visitChainPipe(self, pipe: _pipe.ChainPipe[T]) -> Iterator[T]:
        return itertools.chain(
            pipe.upstream._accept(self), *list(map(iter, pipe.others))
        )

    def visitFilterPipe(self, pipe: _pipe.FilterPipe[T]) -> Iterator[T]:
        return filter(pipe.predicate, pipe.upstream._accept(self))

    def visitBatchPipe(self, pipe: _pipe.BatchPipe[T]) -> Iterator[List[T]]:
        return _core.BatchingIteratorWrapper(
            pipe.upstream._accept(self), pipe.size, pipe.period
        )

    def visitSlowPipe(self, pipe: _pipe.SlowPipe[T]) -> Iterator[T]:
        return _core.SlowingIteratorWrapper(pipe.upstream._accept(self), pipe.freq)

    def visitCatchPipe(self, pipe: _pipe.CatchPipe[T]) -> Iterator[T]:
        return _core.CatchingIteratorWrapper(
            pipe.upstream._accept(self), *pipe.classes, when=pipe.when
        )

    def visitLogPipe(self, pipe: _pipe.LogPipe[T]) -> Iterator[T]:
        return _core.LoggingIteratorWrapper(pipe.upstream._accept(self), pipe.what)
