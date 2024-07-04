from typing import Iterable, Iterator, TypeVar, cast

from streamable import functions, util
from streamable.stream import (
    AForeachStream,
    AMapStream,
    CatchStream,
    FilterStream,
    FlattenStream,
    ForeachStream,
    GroupStream,
    MapStream,
    ObserveStream,
    SlowStream,
    Stream,
    TruncateStream,
)
from streamable.visitor import Visitor

T = TypeVar("T")
U = TypeVar("U")


class IteratorVisitor(Visitor[Iterator[T]]):
    def visit_catch_stream(self, stream: CatchStream[T]) -> Iterator[T]:
        return functions.catch(
            stream.upstream.accept(self),
            stream.kind,
            finally_raise=stream.finally_raise,
        )

    def visit_filter_stream(self, stream: FilterStream[T]) -> Iterator[T]:
        return filter(
            util.reraise_as(stream.keep, StopIteration, functions.NoopStopIteration),
            cast(Iterable[T], stream.upstream.accept(self)),
        )

    def visit_flatten_stream(self, stream: FlattenStream[T]) -> Iterator[T]:
        return functions.flatten(
            stream.upstream.accept(IteratorVisitor[Iterable]()),
            concurrency=stream.concurrency,
        )

    def visit_foreach_stream(self, stream: ForeachStream[T]) -> Iterator[T]:
        return self.visit_map_stream(
            MapStream(
                stream.upstream,
                util.sidify(stream.effect),
                stream.concurrency,
            )
        )

    def visit_aforeach_stream(self, stream: AForeachStream[T]) -> Iterator[T]:
        return self.visit_amap_stream(
            AMapStream(
                stream.upstream,
                util.async_sidify(stream.effect),
                stream.concurrency,
            )
        )

    def visit_group_stream(self, stream: GroupStream[U]) -> Iterator[T]:
        return cast(
            Iterator[T],
            functions.group(
                stream.upstream.accept(IteratorVisitor[U]()),
                stream.size,
                stream.seconds,
                stream.by,
            ),
        )

    def visit_truncate_stream(self, stream: TruncateStream[T]) -> Iterator[T]:
        return functions.truncate(
            stream.upstream.accept(self),
            stream.count,
            stream.when,
        )

    def visit_map_stream(self, stream: MapStream[U, T]) -> Iterator[T]:
        return functions.map(
            stream.transformation,
            stream.upstream.accept(IteratorVisitor[U]()),
            concurrency=stream.concurrency,
        )

    def visit_amap_stream(self, stream: AMapStream[U, T]) -> Iterator[T]:
        return functions.amap(
            stream.transformation,
            stream.upstream.accept(IteratorVisitor[U]()),
            concurrency=stream.concurrency,
        )

    def visit_observe_stream(self, stream: ObserveStream[T]) -> Iterator[T]:
        return functions.observe(
            stream.upstream.accept(self),
            stream.what,
        )

    def visit_slow_stream(self, stream: SlowStream[T]) -> Iterator[T]:
        return functions.slow(stream.upstream.accept(self), stream.frequency)

    def visit_stream(self, stream: Stream[T]) -> Iterator[T]:
        if isinstance(stream.source, Iterable):
            iterable = stream.source
        elif callable(stream.source):
            iterable = stream.source()
            if not isinstance(iterable, Iterable):
                raise TypeError(
                    f"`source` must be either a Callable[[], Iterable] or an Iterable, but got a Callable[[], {type(iterable).__name__}]"
                )
        else:
            raise TypeError(
                f"`source` must be either a Callable[[], Iterable] or an Iterable, but got a {type(stream.source).__name__}"
            )
        return iter(iterable)
