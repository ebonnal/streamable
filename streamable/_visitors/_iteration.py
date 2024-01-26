from typing import Iterable, Iterator, TypeVar, cast

from streamable import _stream, _util, functions
from streamable._visitors._visitor import Visitor

T = TypeVar("T")
U = TypeVar("U")


class IteratorProducingVisitor(Visitor[Iterator[T]]):
    def visit_batch_stream(self, stream: _stream.BatchStream[U]) -> Iterator[T]:
        return cast(
            Iterator[T],
            functions.batch(
                stream.upstream()._accept(IteratorProducingVisitor[U]()),
                stream.size,
                stream.seconds,
            ),
        )

    def visit_catch_stream(self, stream: _stream.CatchStream[T]) -> Iterator[T]:
        return functions.catch(
            stream.upstream()._accept(self),
            *stream.classes,
            when=stream.when,
            raise_at_exhaustion=stream.raise_at_exhaustion,
        )

    def visit_do_stream(self, stream: _stream.DoStream[T]) -> Iterator[T]:
        return self.visit_map_stream(
            _stream.MapStream(
                stream.upstream(),
                _util.sidify(stream.func),
                stream.concurrency,
            )
        )

    def visit_filter_stream(self, stream: _stream.FilterStream[T]) -> Iterator[T]:
        return filter(
            _util.map_exception(stream.predicate, StopIteration, RuntimeError),
            cast(Iterable[T], stream.upstream()._accept(self)),
        )

    def visit_flatten_stream(self, stream: _stream.FlattenStream[T]) -> Iterator[T]:
        return functions.flatten(
            stream.upstream()._accept(IteratorProducingVisitor[Iterable]()),
            concurrency=stream.concurrency,
        )

    def visit_observe_stream(self, stream: _stream.ObserveStream[T]) -> Iterator[T]:
        return functions.observe(
            stream.upstream()._accept(self),
            stream.what,
            stream.colored,
        )

    def visit_map_stream(self, stream: _stream.MapStream[U, T]) -> Iterator[T]:
        return functions.map(
            stream.func,
            stream.upstream()._accept(IteratorProducingVisitor[U]()),
            concurrency=stream.concurrency,
        )

    def visit_slow_stream(self, stream: _stream.SlowStream[T]) -> Iterator[T]:
        return functions.slow(stream.upstream()._accept(self), stream.frequency)

    def visit_stream(self, stream: _stream.Stream[T]) -> Iterator[T]:
        iterable = stream._source()
        _util.validate_iterable(iterable)
        return iter(iterable)
