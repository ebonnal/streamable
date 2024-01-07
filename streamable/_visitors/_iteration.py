import itertools
from typing import Iterable, Iterator, List, TypeVar, cast

from typing_extensions import override

from streamable import _stream, _util, functions
from streamable._visitors._base import Visitor

T = TypeVar("T")
U = TypeVar("U")


class IteratorProducingVisitor(Visitor[Iterator[T]]):
    @override
    def visit_batch_stream(self, stream: _stream.BatchStream[U]) -> Iterator[T]:
        return cast(
            Iterator[T],
            functions.batch(
                stream.upstream()._accept(IteratorProducingVisitor[U]()),
                stream.size,
                stream.seconds,
            ),
        )

    @override
    def visit_catch_stream(self, stream: _stream.CatchStream[T]) -> Iterator[T]:
        return functions.catch(
            stream.upstream()._accept(self),
            *stream.classes,
            when=stream.when,
            raise_at_exhaustion=stream.raise_at_exhaustion,
        )

    @override
    def visit_chain_stream(self, stream: _stream.ChainStream[T]) -> Iterator[T]:
        other_its: List[Iterator[T]] = list(
            map(lambda stream: stream._accept(self), stream.others)
        )
        return itertools.chain(
            cast(Iterable[T], stream.upstream()._accept(self)),
            *other_its,
        )

    @override
    def visit_do_stream(self, stream: _stream.DoStream[T]) -> Iterator[T]:
        return self.visit_map_stream(
            _stream.MapStream(
                stream.upstream(),
                _util.sidify(stream.func),
                stream.concurrency,
            )
        )

    @override
    def visit_filter_stream(self, stream: _stream.FilterStream[T]) -> Iterator[T]:
        return filter(
            _util.map_exception(stream.predicate, StopIteration, RuntimeError),
            cast(Iterable[T], stream.upstream()._accept(self)),
        )

    @override
    def visit_flatten_stream(self, stream: _stream.FlattenStream[T]) -> Iterator[T]:
        return functions.flatten(
            stream.upstream()._accept(IteratorProducingVisitor[Iterable]()),
            concurrency=stream.concurrency,
        )

    @override
    def visit_observe_stream(self, stream: _stream.ObserveStream[T]) -> Iterator[T]:
        return functions.observe(
            stream.upstream()._accept(self),
            stream.what,
            stream.colored,
        )

    @override
    def visit_map_stream(self, stream: _stream.MapStream[U, T]) -> Iterator[T]:
        return functions.map(
            stream.func,
            stream.upstream()._accept(IteratorProducingVisitor[U]()),
            concurrency=stream.concurrency,
        )

    @override
    def visit_slow_stream(self, stream: _stream.SlowStream[T]) -> Iterator[T]:
        return functions.slow(stream.upstream()._accept(self), stream.frequency)

    @override
    def visit_stream(self, stream: _stream.Stream[T]) -> Iterator[T]:
        iterable = stream.source()
        _util.validate_iterable(iterable)
        return iter(iterable)
