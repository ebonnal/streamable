from typing import AsyncIterable, Iterable, AsyncIterator, TypeVar, cast

from streamable import afunctions
from streamable.aiterators import SyncToAsyncIterator
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


class AsyncIteratorVisitor(Visitor[AsyncIterator[T]]):
    def visit_catch_stream(self, stream: CatchStream[T]) -> AsyncIterator[T]:
        return afunctions.catch(
            stream.upstream.accept(self),
            stream._errors,
            when=stream._when,
            replacement=stream._replacement,
            finally_raise=stream._finally_raise,
        )

    def visit_distinct_stream(self, stream: DistinctStream[T]) -> AsyncIterator[T]:
        return afunctions.distinct(
            stream.upstream.accept(self),
            stream._key,
            consecutive_only=stream._consecutive_only,
        )

    def visit_filter_stream(self, stream: FilterStream[T]) -> AsyncIterator[T]:
        return afunctions.filter(stream.upstream.accept(self), stream._when)

    def visit_flatten_stream(self, stream: FlattenStream[T]) -> AsyncIterator[T]:
        return afunctions.flatten(
            stream.upstream.accept(AsyncIteratorVisitor[Iterable]()),
            concurrency=stream._concurrency,
        )

    def visit_foreach_stream(self, stream: ForeachStream[T]) -> AsyncIterator[T]:
        return self.visit_map_stream(
            MapStream(
                stream.upstream,
                sidify(stream._effect),
                stream._concurrency,
                stream._ordered,
                stream._via,
            )
        )

    def visit_aforeach_stream(self, stream: AForeachStream[T]) -> AsyncIterator[T]:
        return self.visit_amap_stream(
            AMapStream(
                stream.upstream,
                async_sidify(stream._effect),
                stream._concurrency,
                stream._ordered,
            )
        )

    def visit_group_stream(self, stream: GroupStream[U]) -> AsyncIterator[T]:
        return cast(
            AsyncIterator[T],
            afunctions.group(
                stream.upstream.accept(AsyncIteratorVisitor[U]()),
                stream._size,
                interval=stream._interval,
                by=stream._by,
            ),
        )

    def visit_groupby_stream(self, stream: GroupbyStream[U, T]) -> AsyncIterator[T]:
        return cast(
            AsyncIterator[T],
            afunctions.groupby(
                stream.upstream.accept(AsyncIteratorVisitor[U]()),
                stream._key,
                size=stream._size,
                interval=stream._interval,
            ),
        )

    def visit_map_stream(self, stream: MapStream[U, T]) -> AsyncIterator[T]:
        return afunctions.map(
            stream._transformation,
            stream.upstream.accept(AsyncIteratorVisitor[U]()),
            concurrency=stream._concurrency,
            ordered=stream._ordered,
            via=stream._via,
        )

    def visit_amap_stream(self, stream: AMapStream[U, T]) -> AsyncIterator[T]:
        return afunctions.amap(
            stream._transformation,
            stream.upstream.accept(AsyncIteratorVisitor[U]()),
            concurrency=stream._concurrency,
            ordered=stream._ordered,
        )

    def visit_observe_stream(self, stream: ObserveStream[T]) -> AsyncIterator[T]:
        return afunctions.observe(
            stream.upstream.accept(self),
            stream._what,
        )

    def visit_skip_stream(self, stream: SkipStream[T]) -> AsyncIterator[T]:
        return afunctions.skip(
            stream.upstream.accept(self),
            stream._count,
            until=stream._until,
        )

    def visit_throttle_stream(self, stream: ThrottleStream[T]) -> AsyncIterator[T]:
        return afunctions.throttle(
            stream.upstream.accept(self),
            stream._count,
            per=stream._per,
        )

    def visit_truncate_stream(self, stream: TruncateStream[T]) -> AsyncIterator[T]:
        return afunctions.truncate(
            stream.upstream.accept(self),
            stream._count,
            when=stream._when,
        )

    def visit_stream(self, stream: Stream[T]) -> AsyncIterator[T]:
        if isinstance(stream.source, Iterable):
            return SyncToAsyncIterator(iter(stream.source))
        elif isinstance(stream.source, AsyncIterable):
            return stream.source.__aiter__()
        elif callable(stream.source):
            iterable = stream.source()
            if isinstance(iterable, Iterable):
                return SyncToAsyncIterator(iter(iterable))
            elif isinstance(iterable, AsyncIterable):
                return iterable.__aiter__()
            if not isinstance(iterable, Iterable):
                raise TypeError(
                    f"`source`'s must be an Iterable/AsyncIterable or a Callable[[], Iterable/AsyncIterable] but got a Callable[[], {type(iterable)}]"
                )
        else:
            raise TypeError(
                f"`source` must be an Iterable/AsyncIterable or a Callable[[], Iterable/AsyncIterable] but got a {type(stream.source)}"
            )
