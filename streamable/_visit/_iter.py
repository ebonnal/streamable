import itertools
from typing import Iterable, Iterator, List, TypeVar, cast

from streamable import _stream, _util
from streamable._execution import _concurrency, _core
from streamable._visit._base import Visitor

T = TypeVar("T")
U = TypeVar("U")


class IteratorProducingVisitor(Visitor[Iterator[T]]):
    def visit_source_stream(self, stream: _stream.Stream[T]) -> Iterator[T]:
        iterable = stream.source()
        _util.validate_iterable(iterable)
        return iter(iterable)

    def visit_map_stream(self, stream: _stream.MapStream[U, T]) -> Iterator[T]:
        func = _util.map_exception(
            stream.func, source=StopIteration, target=RuntimeError
        )
        it: Iterator[U] = stream.upstream._accept(IteratorProducingVisitor[U]())
        if stream.concurrency == 1:
            return map(func, it)
        else:
            return _concurrency.RaisingIterator(
                iter(
                    _concurrency.ThreadedMappingIterable(
                        it, func, n_workers=stream.concurrency
                    )
                )
            )

    def visit_do_stream(self, stream: _stream.DoStream[T]) -> Iterator[T]:
        func = _util.sidify(
            _util.map_exception(stream.func, source=StopIteration, target=RuntimeError)
        )
        return self.visit_map_stream(
            _stream.MapStream(stream.upstream, func, stream.concurrency)
        )

    def visit_flatten_stream(self, stream: _stream.FlattenStream[T]) -> Iterator[T]:
        it = stream.upstream._accept(IteratorProducingVisitor[Iterable]())
        if stream.concurrency == 1:
            return _core.FlatteningIterator(it)
        else:
            return _concurrency.RaisingIterator(
                iter(
                    _concurrency.ThreadedFlatteningIterable(
                        it, n_workers=stream.concurrency
                    )
                )
            )

    def visit_chain_stream(self, stream: _stream.ChainStream[T]) -> Iterator[T]:
        it: Iterator[T] = stream.upstream._accept(self)
        other_its: List[Iterator[T]] = list(
            map(lambda stream: stream._accept(self), stream.others)
        )
        return itertools.chain(it, *other_its)

    def visit_filter_stream(self, stream: _stream.FilterStream[T]) -> Iterator[T]:
        predicate = _util.map_exception(
            stream.predicate, source=StopIteration, target=RuntimeError
        )
        it: Iterator[T] = stream.upstream._accept(self)
        return filter(predicate, it)

    def visit_batch_stream(self, stream: _stream.BatchStream[U]) -> Iterator[T]:
        it: Iterator[U] = stream.upstream._accept(IteratorProducingVisitor[U]())
        return cast(
            Iterator[T], _core.BatchingIterator(it, stream.size, stream.seconds)
        )

    def visit_slow_stream(self, stream: _stream.SlowStream[T]) -> Iterator[T]:
        return _core.SlowingIterator(stream.upstream._accept(self), stream.frequency)

    def visit_catch_stream(self, stream: _stream.CatchStream[T]) -> Iterator[T]:
        if stream.when is not None:
            when = _util.map_exception(
                stream.when, source=StopIteration, target=RuntimeError
            )
        else:
            when = None
        return _core.CatchingIterator(
            stream.upstream._accept(self), *stream.classes, when=when
        )

    def visit_observe_stream(self, stream: _stream.ObserveStream[T]) -> Iterator[T]:
        return _core.ObservingIterator(
            stream.upstream._accept(self), stream.what, stream.colored
        )
