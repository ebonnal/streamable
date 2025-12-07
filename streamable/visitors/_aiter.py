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
    CatchStream,
    DoStream,
    FilterStream,
    FlattenStream,
    GroupbyStream,
    GroupStream,
    MapStream,
    ObserveStream,
    SkipStream,
    stream,
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
        if (
            not iscoroutinefunction(stream._when)
            and not iscoroutinefunction(stream._replace)
            and not iscoroutinefunction(stream._do)
        ):
            return _afunctions.catch(
                stream.upstream.accept(cast(AsyncIteratorVisitor[Union[T, U]], self)),
                stream._errors,
                when=stream._when,
                replace=cast(Callable[[Exception], U], stream._replace),
                do=stream._do,
                finally_raise=stream._finally_raise,
            )
        elif (
            (not stream._when or iscoroutinefunction(stream._when))
            and (not stream._replace or iscoroutinefunction(stream._replace))
            and (not stream._do or iscoroutinefunction(stream._do))
        ):
            return _afunctions.acatch(
                stream.upstream.accept(cast(AsyncIteratorVisitor[Union[T, U]], self)),
                stream._errors,
                when=stream._when,
                replace=cast(
                    Callable[[Exception], Coroutine[Any, Any, U]], stream._replace
                ),
                do=stream._do,
                finally_raise=stream._finally_raise,
            )
        raise TypeError(
            "`when`/`replace`/`do` must all be coroutine functions or neither should be"
        )

    def visit_filter_stream(self, stream: FilterStream[T]) -> AsyncIterator[T]:
        if iscoroutinefunction(stream._where):
            return _afunctions.afilter(stream.upstream.accept(self), stream._where)
        return _afunctions.filter(stream.upstream.accept(self), stream._where)

    def visit_flatten_stream(self, stream: FlattenStream[T]) -> AsyncIterator[T]:
        return _afunctions.flatten(
            stream.upstream.accept(
                cast(AsyncIteratorVisitor[Union[Iterable[T], AsyncIterable[T]]], self)
            ),
            concurrency=stream._concurrency,
        )

    def visit_do_stream(self, stream: DoStream[T]) -> AsyncIterator[T]:
        if iscoroutinefunction(stream._effect):
            return self.visit_map_stream(
                MapStream(
                    stream.upstream,
                    async_sidify(stream._effect),
                    stream._concurrency,
                    stream._ordered,
                )
            )
        return self.visit_map_stream(
            MapStream(
                stream.upstream,
                sidify(cast(Callable[[T], Any], stream._effect)),
                stream._concurrency,
                stream._ordered,
            )
        )

    def visit_group_stream(self, stream: GroupStream[U]) -> AsyncIterator[T]:
        if iscoroutinefunction(stream._by):
            return cast(
                AsyncIterator[T],
                _afunctions.agroup(
                    stream.upstream.accept(cast(AsyncIteratorVisitor[U], self)),
                    stream._up_to,
                    over=stream._over,
                    by=stream._by,
                ),
            )
        return cast(
            AsyncIterator[T],
            _afunctions.group(
                stream.upstream.accept(cast(AsyncIteratorVisitor[U], self)),
                stream._up_to,
                over=stream._over,
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
                    up_to=stream._up_to,
                    over=stream._over,
                ),
            )
        return cast(
            AsyncIterator[T],
            _afunctions.groupby(
                stream.upstream.accept(cast(AsyncIteratorVisitor[U], self)),
                stream._key,
                up_to=stream._up_to,
                over=stream._over,
            ),
        )

    def visit_map_stream(self, stream: MapStream[U, T]) -> AsyncIterator[T]:
        if iscoroutinefunction(stream._into):
            return _afunctions.amap(
                stream._into,
                stream.upstream.accept(cast(AsyncIteratorVisitor[U], self)),
                concurrency=cast(int, stream._concurrency),
                ordered=stream._ordered,
            )
        return _afunctions.map(
            cast(Callable[[U], T], stream._into),
            stream.upstream.accept(cast(AsyncIteratorVisitor[U], self)),
            concurrency=stream._concurrency,
            ordered=stream._ordered,
        )

    def visit_observe_stream(self, stream: ObserveStream[T]) -> AsyncIterator[T]:
        return _afunctions.observe(
            stream.upstream.accept(self),
            stream._label,
            stream._every,
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

    def visit_stream(self, stream: stream[T]) -> AsyncIterator[T]:
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
                f"if `source` is callable it must return an Iterable or AsyncIterable but got {type(iterable)}"
            )
        raise TypeError(
            f"`source` must be Iterable or AsyncIterable or Callable but got {type(stream.source)}"
        )
