from typing import AsyncIterable, AsyncIterator, Iterable, TypeVar, cast

from streamable import afunctions
from streamable.stream import (
    ACatchStream,
    ADistinctStream,
    AFilterStream,
    AFlattenStream,
    AForeachStream,
    AGroupbyStream,
    AGroupStream,
    AMapStream,
    ASkipStream,
    ATruncateStream,
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
from streamable.util.functiontools import async_sidify, sidify
from streamable.util.iterabletools import sync_to_async_iter
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

    def visit_acatch_stream(self, stream: ACatchStream[T]) -> AsyncIterator[T]:
        return afunctions.acatch(
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

    def visit_adistinct_stream(self, stream: ADistinctStream[T]) -> AsyncIterator[T]:
        return afunctions.adistinct(
            stream.upstream.accept(self),
            stream._key,
            consecutive_only=stream._consecutive_only,
        )

    def visit_filter_stream(self, stream: FilterStream[T]) -> AsyncIterator[T]:
        return afunctions.filter(stream.upstream.accept(self), stream._when)

    def visit_afilter_stream(self, stream: AFilterStream[T]) -> AsyncIterator[T]:
        return afunctions.afilter(stream.upstream.accept(self), stream._when)

    def visit_flatten_stream(self, stream: FlattenStream[T]) -> AsyncIterator[T]:
        return afunctions.flatten(
            stream.upstream.accept(AsyncIteratorVisitor[Iterable]()),
            concurrency=stream._concurrency,
        )

    def visit_aflatten_stream(self, stream: AFlattenStream[T]) -> AsyncIterator[T]:
        return afunctions.aflatten(
            stream.upstream.accept(AsyncIteratorVisitor[AsyncIterable]()),
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

    def visit_agroup_stream(self, stream: AGroupStream[U]) -> AsyncIterator[T]:
        return cast(
            AsyncIterator[T],
            afunctions.agroup(
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

    def visit_agroupby_stream(self, stream: AGroupbyStream[U, T]) -> AsyncIterator[T]:
        return cast(
            AsyncIterator[T],
            afunctions.agroupby(
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

    def visit_askip_stream(self, stream: ASkipStream[T]) -> AsyncIterator[T]:
        return afunctions.askip(
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

    def visit_atruncate_stream(self, stream: ATruncateStream[T]) -> AsyncIterator[T]:
        return afunctions.atruncate(
            stream.upstream.accept(self),
            stream._count,
            when=stream._when,
        )

    def visit_stream(self, stream: Stream[T]) -> AsyncIterator[T]:
        if isinstance(stream.source, Iterable):
            return sync_to_async_iter(stream.source)
        if isinstance(stream.source, AsyncIterable):
            return stream.source.__aiter__()
        if callable(stream.source):
            iterable = stream.source()
            if isinstance(iterable, Iterable):
                return sync_to_async_iter(iterable)
            if isinstance(iterable, AsyncIterable):
                return iterable.__aiter__()
            raise TypeError(
                f"`source` must be an Iterable/AsyncIterable or a Callable[[], Iterable/AsyncIterable] but got a Callable[[], {type(iterable)}]"
            )
        raise TypeError(
            f"`source` must be an Iterable/AsyncIterable or a Callable[[], Iterable/AsyncIterable] but got a {type(stream.source)}"
        )
