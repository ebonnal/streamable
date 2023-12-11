import itertools
from typing import Iterable, Iterator, List, TypeVar, cast

from kioss import _pipe, _util
from kioss._execution import _concurrency, _core
from kioss._visit._base import Visitor

T = TypeVar("T")
U = TypeVar("U")


class IteratorProducingVisitor(Visitor[Iterator[T]]):
    def visit_source_pipe(self, pipe: _pipe.Pipe[T]) -> Iterator[T]:
        iterable = pipe.source()
        _util.ducktype_assert_iterable(iterable)
        return iter(iterable)

    def visit_map_pipe(self, pipe: _pipe.MapPipe[U, T]) -> Iterator[T]:
        func = _util.map_exception(pipe.func, source=StopIteration, target=RuntimeError)
        it: Iterator[U] = pipe.upstream._accept(IteratorProducingVisitor[U]())
        if pipe.n_threads == 1:
            return map(func, it)
        else:
            return _concurrency.ThreadedMappingIteratorWrapper(
                it, func, n_workers=pipe.n_threads
            )

    def visit_do_pipe(self, pipe: _pipe.DoPipe[T]) -> Iterator[T]:
        return self.visit_map_pipe(
            _pipe.MapPipe(pipe.upstream, _util.sidify(pipe.func), pipe.n_threads)
        )

    def visit_flatten_pipe(self, pipe: _pipe.FlattenPipe[T]) -> Iterator[T]:
        it = pipe.upstream._accept(IteratorProducingVisitor[Iterable]())
        if pipe.n_threads == 1:
            return _core.FlatteningIteratorWrapper(it)
        else:
            return _concurrency.ThreadedFlatteningIteratorWrapper(
                it, n_workers=pipe.n_threads
            )

    def visit_chain_pipe(self, pipe: _pipe.ChainPipe[T]) -> Iterator[T]:
        it: Iterator[T] = pipe.upstream._accept(self)
        other_its: List[Iterator[T]] = list(
            map(lambda pipe: pipe._accept(self), pipe.others)
        )
        return itertools.chain(it, *other_its)

    def visit_filter_pipe(self, pipe: _pipe.FilterPipe[T]) -> Iterator[T]:
        it: Iterator[T] = pipe.upstream._accept(self)
        return filter(pipe.predicate, it)

    def visit_batch_pipe(self, pipe: _pipe.BatchPipe[U]) -> Iterator[T]:
        it: Iterator[U] = pipe.upstream._accept(IteratorProducingVisitor[U]())
        return cast(
            Iterator[T], _core.BatchingIteratorWrapper(it, pipe.size, pipe.period)
        )

    def visit_slow_pipe(self, pipe: _pipe.SlowPipe[T]) -> Iterator[T]:
        return _core.SlowingIteratorWrapper(pipe.upstream._accept(self), pipe.freq)

    def visit_catch_pipe(self, pipe: _pipe.CatchPipe[T]) -> Iterator[T]:
        return _core.CatchingIteratorWrapper(
            pipe.upstream._accept(self), *pipe.classes, when=pipe.when
        )

    def visit_log_pipe(self, pipe: _pipe.LogPipe[T]) -> Iterator[T]:
        return _core.LoggingIteratorWrapper(
            pipe.upstream._accept(self), pipe.what, pipe.colored
        )
