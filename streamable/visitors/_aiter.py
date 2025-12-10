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
from streamable._utils._iter import afn_to_aiter, fn_to_aiter, sync_to_async_iter
from streamable.visitors import Visitor

T = TypeVar("T")
U = TypeVar("U")


class AsyncIteratorVisitor(Visitor[AsyncIterator[T]]):
    def visit_catch_stream(
        self, stream: CatchStream[T, U]
    ) -> AsyncIterator[Union[T, U]]:
        return _afunctions.catch(
            stream.upstream.accept(cast(AsyncIteratorVisitor[Union[T, U]], self)),
            stream._errors,
            when=stream._when,
            replace=stream._replace,
            do=stream._do,
            finally_raise=stream._finally_raise,
            terminate=stream._terminate,
        )

    def visit_filter_stream(self, stream: FilterStream[T]) -> AsyncIterator[T]:
        return _afunctions.filter(stream._where, stream.upstream.accept(self))

    def visit_flatten_stream(self, stream: FlattenStream[T]) -> AsyncIterator[T]:
        return _afunctions.flatten(
            stream.upstream.accept(
                cast(AsyncIteratorVisitor[Union[Iterable[T], AsyncIterable[T]]], self)
            ),
            concurrency=stream._concurrency,
        )

    def visit_do_stream(self, stream: DoStream[T]) -> AsyncIterator[T]:
        return self.visit_map_stream(
            MapStream(
                stream.upstream,
                async_sidify(stream._effect)
                if iscoroutinefunction(stream._effect)
                else sidify(stream._effect),
                stream._concurrency,
                stream._ordered,
            )
        )

    def visit_group_stream(self, stream: GroupStream[U]) -> AsyncIterator[T]:
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
        return cast(
            AsyncIterator[T],
            _afunctions.groupby(
                stream.upstream.accept(cast(AsyncIteratorVisitor[U], self)),
                stream._by,
                up_to=stream._up_to,
                over=stream._over,
            ),
        )

    def visit_map_stream(self, stream: MapStream[U, T]) -> AsyncIterator[T]:
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
            if iscoroutinefunction(stream.source):
                return afn_to_aiter(
                    cast(Callable[[], Coroutine[Any, Any, T]], stream.source)
                )
            else:
                return fn_to_aiter(stream.source)
        raise TypeError(
            f"`source` must be Iterable or AsyncIterable or Callable but got {type(stream.source)}"
        )
