from typing import Iterable, Iterator, TypeVar, cast

from streamable import functions
from streamable.stream import (
    AForeachStream,
    AMapStream,
    CatchStream,
    DistinctStream,
    FilterStream,
    FlattenStream,
    ForeachStream,
    GroupbyStream,
    GroupStream,
    MapStream,
    ObserveStream,
    SkipStream,
    Stream,
    ThrottleStream,
    TruncateStream,
)
from streamable.util.functiontools import async_sidify, sidify, wrap_error
from streamable.visitors import Visitor

T = TypeVar("T")
U = TypeVar("U")


class IteratorVisitor(Visitor[Iterator[T]]):
    def visit_catch_stream(self, stream: CatchStream[T]) -> Iterator[T]:
        return functions.catch(
            cast(Iterable, stream.upstream.accept(self)),
            stream._error_type,
            *stream._others,
            when=stream._when,
            replacement=stream._replacement,
            finally_raise=stream._finally_raise,
        )

    def visit_distinct_stream(self, stream: DistinctStream[T]) -> Iterator[T]:
        return functions.distinct(
            cast(Iterable, stream.upstream.accept(self)),
            stream._key,
            stream._consecutive_only,
        )

    def visit_filter_stream(self, stream: FilterStream[T]) -> Iterator[T]:
        return filter(
            wrap_error(stream._when, StopIteration) if stream._when else None,
            cast(Iterable[T], cast(Iterable, stream.upstream.accept(self))),
        )

    def visit_flatten_stream(self, stream: FlattenStream[T]) -> Iterator[T]:
        return functions.flatten(
            cast(Iterable, stream.upstream.accept(IteratorVisitor[Iterable]())),
            concurrency=stream._concurrency,
        )

    def visit_foreach_stream(self, stream: ForeachStream[T]) -> Iterator[T]:
        return self.visit_map_stream(
            MapStream(
                stream.upstream,
                sidify(stream._effect),
                stream._concurrency,
                stream._ordered,
                stream._via,
            )
        )

    def visit_aforeach_stream(self, stream: AForeachStream[T]) -> Iterator[T]:
        return self.visit_amap_stream(
            AMapStream(
                stream.upstream,
                async_sidify(stream._effect),
                stream._concurrency,
                stream._ordered,
            )
        )

    def visit_group_stream(self, stream: GroupStream[U]) -> Iterator[T]:
        return cast(
            Iterator[T],
            functions.group(
                cast(Iterable, stream.upstream.accept(IteratorVisitor[U]())),
                stream._size,
                stream._interval,
                stream._by,
            ),
        )

    def visit_groupby_stream(self, stream: GroupbyStream[U, T]) -> Iterator[T]:
        return cast(
            Iterator[T],
            functions.groupby(
                cast(Iterable, stream.upstream.accept(IteratorVisitor[U]())),
                stream._key,
                stream._size,
                stream._interval,
            ),
        )

    def visit_map_stream(self, stream: MapStream[U, T]) -> Iterator[T]:
        return functions.map(
            stream._transformation,
            cast(Iterable, stream.upstream.accept(IteratorVisitor[U]())),
            concurrency=stream._concurrency,
            ordered=stream._ordered,
            via=stream._via,
        )

    def visit_amap_stream(self, stream: AMapStream[U, T]) -> Iterator[T]:
        return functions.amap(
            stream._transformation,
            cast(Iterable, stream.upstream.accept(IteratorVisitor[U]())),
            concurrency=stream._concurrency,
            ordered=stream._ordered,
        )

    def visit_observe_stream(self, stream: ObserveStream[T]) -> Iterator[T]:
        return functions.observe(
            cast(Iterable, stream.upstream.accept(self)),
            stream._what,
        )

    def visit_skip_stream(self, stream: SkipStream[T]) -> Iterator[T]:
        return functions.skip(
            cast(Iterable, stream.upstream.accept(self)),
            stream._count,
            stream._until,
        )

    def visit_throttle_stream(self, stream: ThrottleStream[T]) -> Iterator[T]:
        return functions.throttle(
            cast(Iterable, stream.upstream.accept(self)),
            stream._count,
            stream._per,
        )

    def visit_truncate_stream(self, stream: TruncateStream[T]) -> Iterator[T]:
        return functions.truncate(
            cast(Iterable, stream.upstream.accept(self)),
            stream._count,
            stream._when,
        )

    def visit_stream(self, stream: Stream[T]) -> Iterator[T]:
        if isinstance(stream.source, Iterable):
            iterable = stream.source
        elif callable(stream.source):
            iterable = stream.source()
            if not isinstance(iterable, Iterable):
                raise TypeError(
                    f"`source` must be an Iterable or a Callable[[], Iterable] but got a Callable[[], {type(iterable)}]"
                )
        else:
            raise TypeError(
                f"`source` must be an Iterable or a Callable[[], Iterable] but got a {type(stream.source)}"
            )
        return iter(iterable)
