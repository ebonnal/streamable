from inspect import iscoroutinefunction
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Coroutine,
    Iterable,
    TypeVar,
    Union,
    cast,
)

from streamable import _afunctions
from streamable._stream import (
    AFlattenStream,
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
from streamable._utils._func import async_sidify, sidify
from streamable._utils._iter import sync_to_async_iter
from streamable.visitors import Visitor

T = TypeVar("T")
U = TypeVar("U")


class AsyncIteratorVisitor(Visitor[AsyncIterator[T]]):
    def visit_catch_stream(
        self, stream: CatchStream[T, U]
    ) -> AsyncIterator[Union[T, U]]:
        if (not stream._when or not iscoroutinefunction(stream._when)) and (
            not stream._replace or not iscoroutinefunction(stream._replace)
        ):
            return _afunctions.catch(
                stream.upstream.accept(cast(AsyncIteratorVisitor[Union[T, U]], self)),
                stream._errors,
                when=stream._when,
                replace=cast(Callable[[Exception], U], stream._replace),
                finally_raise=stream._finally_raise,
            )
        elif (not stream._when or iscoroutinefunction(stream._when)) and (
            not stream._replace or iscoroutinefunction(stream._replace)
        ):
            return _afunctions.acatch(
                stream.upstream.accept(cast(AsyncIteratorVisitor[Union[T, U]], self)),
                stream._errors,
                when=stream._when,
                replace=cast(
                    Callable[[Exception], Coroutine[Any, Any, U]], stream._replace
                ),
                finally_raise=stream._finally_raise,
            )
        raise TypeError(
            "`when` and `replace` must both be coroutine functions or neither should be"
        )

    def visit_distinct_stream(self, stream: DistinctStream[T]) -> AsyncIterator[T]:
        if stream._by is None:
            return _afunctions.distinct(
                stream.upstream.accept(self),
                stream._by,
                consecutive=stream._consecutive,
            )
        if iscoroutinefunction(stream._by):
            return _afunctions.adistinct(
                stream.upstream.accept(self),
                stream._by,
                consecutive=stream._consecutive,
            )
        return _afunctions.distinct(
            stream.upstream.accept(self),
            stream._by,
            consecutive=stream._consecutive,
        )

    def visit_filter_stream(self, stream: FilterStream[T]) -> AsyncIterator[T]:
        if iscoroutinefunction(stream._where):
            return _afunctions.afilter(stream.upstream.accept(self), stream._where)
        return _afunctions.filter(stream.upstream.accept(self), stream._where)

    def visit_flatten_stream(self, stream: FlattenStream[T]) -> AsyncIterator[T]:
        return _afunctions.flatten(
            stream.upstream.accept(cast(AsyncIteratorVisitor[Iterable], self)),
            concurrency=stream._concurrency,
        )

    def visit_aflatten_stream(self, stream: AFlattenStream[T]) -> AsyncIterator[T]:
        return _afunctions.aflatten(
            stream.upstream.accept(cast(AsyncIteratorVisitor[AsyncIterable], self)),
            concurrency=stream._concurrency,
        )

    def visit_foreach_stream(self, stream: ForeachStream[T]) -> AsyncIterator[T]:
        if iscoroutinefunction(stream._do):
            return self.visit_map_stream(
                MapStream(
                    stream.upstream,
                    async_sidify(stream._do),
                    stream._concurrency,
                    stream._ordered,
                    stream._via,
                )
            )
        return self.visit_map_stream(
            MapStream(
                stream.upstream,
                sidify(cast(Callable[[T], Any], stream._do)),
                stream._concurrency,
                stream._ordered,
                stream._via,
            )
        )

    def visit_group_stream(self, stream: GroupStream[U]) -> AsyncIterator[T]:
        if iscoroutinefunction(stream._by):
            return cast(
                AsyncIterator[T],
                _afunctions.agroup(
                    stream.upstream.accept(cast(AsyncIteratorVisitor[U], self)),
                    stream._size,
                    interval=stream._interval,
                    by=stream._by,
                ),
            )
        return cast(
            AsyncIterator[T],
            _afunctions.group(
                stream.upstream.accept(cast(AsyncIteratorVisitor[U], self)),
                stream._size,
                interval=stream._interval,
                by=stream._by,
            ),
        )

    def visit_groupby_stream(self, stream: GroupbyStream[U, T]) -> AsyncIterator[T]:
        if iscoroutinefunction(stream._key):
            return cast(
                AsyncIterator[T],
                _afunctions.agroupby(
                    stream.upstream.accept(cast(AsyncIteratorVisitor[U], self)),
                    stream._key,
                    size=stream._size,
                    interval=stream._interval,
                ),
            )
        return cast(
            AsyncIterator[T],
            _afunctions.groupby(
                stream.upstream.accept(cast(AsyncIteratorVisitor[U], self)),
                stream._key,
                size=stream._size,
                interval=stream._interval,
            ),
        )

    def visit_map_stream(self, stream: MapStream[U, T]) -> AsyncIterator[T]:
        if iscoroutinefunction(stream._to):
            return _afunctions.amap(
                stream._to,
                stream.upstream.accept(cast(AsyncIteratorVisitor[U], self)),
                concurrency=stream._concurrency,
                ordered=stream._ordered,
            )
        return _afunctions.map(
            cast(Callable[[U], T], stream._to),
            stream.upstream.accept(cast(AsyncIteratorVisitor[U], self)),
            concurrency=stream._concurrency,
            ordered=stream._ordered,
            via=stream._via,
        )

    def visit_observe_stream(self, stream: ObserveStream[T]) -> AsyncIterator[T]:
        return _afunctions.observe(
            stream.upstream.accept(self),
            stream._what,
        )

    def visit_skip_stream(self, stream: SkipStream[T]) -> AsyncIterator[T]:
        if isinstance(stream._until, int):
            return _afunctions.skip(
                stream.upstream.accept(self),
                until=stream._until,
            )
        if iscoroutinefunction(stream._until):
            return _afunctions.askip(
                stream.upstream.accept(self),
                until=stream._until,
            )
        return _afunctions.skip(
            stream.upstream.accept(self),
            until=stream._until,
        )

    def visit_throttle_stream(self, stream: ThrottleStream[T]) -> AsyncIterator[T]:
        return _afunctions.throttle(
            stream.upstream.accept(self),
            stream._up_to,
            per=stream._per,
        )

    def visit_truncate_stream(self, stream: TruncateStream[T]) -> AsyncIterator[T]:
        if isinstance(stream._when, int):
            return _afunctions.truncate(
                stream.upstream.accept(self),
                when=stream._when,
            )
        if iscoroutinefunction(stream._when):
            return _afunctions.atruncate(
                stream.upstream.accept(self),
                when=stream._when,
            )
        return _afunctions.truncate(
            stream.upstream.accept(self),
            when=stream._when,
        )

    def visit_stream(self, stream: Stream[T]) -> AsyncIterator[T]:
        if isinstance(stream.source, Iterable):
            return sync_to_async_iter(stream.source.__iter__())
        if isinstance(stream.source, AsyncIterable):
            return stream.source.__aiter__()
        if callable(stream.source):
            iterable = stream.source()
            if isinstance(iterable, Iterable):
                return sync_to_async_iter(iterable.__iter__())
            if isinstance(iterable, AsyncIterable):
                return iterable.__aiter__()
            raise TypeError(
                f"`source` must be an Iterable/AsyncIterable or a Callable[[], Iterable/AsyncIterable] but got a Callable[[], {type(iterable)}]"
            )
        raise TypeError(
            f"`source` must be an Iterable/AsyncIterable or a Callable[[], Iterable/AsyncIterable] but got a {type(stream.source)}"
        )
