from inspect import iscoroutinefunction
from typing import (
    TYPE_CHECKING,
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
from streamable._tools._func import sidify
from streamable._tools._iter import (
    ClosableAsyncIterator,
    NoopClosableAsyncIterator,
    afn_to_aiter,
    fn_to_aiter,
    async_iter,
)
from streamable.visitors import Visitor

if TYPE_CHECKING:  # pragma: no cover
    from streamable._stream import (
        BufferStream,
        CatchStream,
        DoStream,
        FilterStream,
        FlattenStream,
        GroupStream,
        MapStream,
        ObserveStream,
        SkipStream,
        stream,
        ThrottleStream,
        TakeStream,
    )


T = TypeVar("T")
U = TypeVar("U")


class AsyncIteratorVisitor(Visitor[ClosableAsyncIterator[T]]):
    __slots__ = ()

    def visit_buffer_stream(self, s: "BufferStream[T]") -> ClosableAsyncIterator[T]:
        return _afunctions.buffer(
            s.upstream.accept(self),
            s._up_to,
        )

    def visit_catch_stream(
        self, s: "CatchStream[T, U]"
    ) -> ClosableAsyncIterator[Union[T, U]]:
        return _afunctions.catch(
            s.upstream.accept(self),
            s._errors,
            where=s._where,
            replace=s._replace,
            do=s._do,
            stop=s._stop,
        )

    def visit_do_stream(self, s: "DoStream[T]") -> ClosableAsyncIterator[T]:
        return _afunctions.map(
            sidify(s._effect),
            s.upstream.accept(self),
            concurrency=s._concurrency,
            as_completed=s._as_completed,
        )

    def visit_filter_stream(self, s: "FilterStream[T]") -> ClosableAsyncIterator[T]:
        return _afunctions.filter(s._where, s.upstream.accept(self))

    def visit_flatten_stream(self, s: "FlattenStream[T]") -> ClosableAsyncIterator[T]:
        return _afunctions.flatten(
            s.upstream.accept(
                cast(AsyncIteratorVisitor[Union[Iterable[T], AsyncIterable[T]]], self)
            ),
            concurrency=s._concurrency,
        )

    def visit_group_stream(self, s: "GroupStream[T]") -> ClosableAsyncIterator[T]:
        return cast(
            ClosableAsyncIterator[T],
            _afunctions.group(
                s.upstream.accept(self),
                s._up_to,
                within=s._within,
                by=s._by,
            ),
        )

    def visit_map_stream(self, s: "MapStream[U, T]") -> ClosableAsyncIterator[T]:
        return _afunctions.map(
            s._into,
            s.upstream.accept(cast(AsyncIteratorVisitor[U], self)),
            concurrency=s._concurrency,
            as_completed=s._as_completed,
        )

    def visit_observe_stream(self, s: "ObserveStream[T]") -> ClosableAsyncIterator[T]:
        return _afunctions.observe(
            s.upstream.accept(self),
            s._subject,
            s._every,
            s._do,
        )

    def visit_skip_stream(self, s: "SkipStream[T]") -> ClosableAsyncIterator[T]:
        return _afunctions.skip(
            s.upstream.accept(self),
            until=s._until,
        )

    def visit_take_stream(self, s: "TakeStream[T]") -> ClosableAsyncIterator[T]:
        return _afunctions.take(
            s.upstream.accept(self),
            until=s._until,
        )

    def visit_throttle_stream(self, s: "ThrottleStream[T]") -> ClosableAsyncIterator[T]:
        return _afunctions.throttle(
            s.upstream.accept(self),
            s._up_to,
            per=s._per,
        )

    def visit_stream(self, s: "stream[T]") -> ClosableAsyncIterator[T]:
        upstream: AsyncIterator[T]
        if isinstance(s.source, (Iterable, AsyncIterable)):
            upstream = async_iter(s.source)
        elif callable(s.source):
            if iscoroutinefunction(s.source):
                upstream = afn_to_aiter(
                    cast(Callable[[], Coroutine[object, object, T]], s.source)
                )
            else:
                upstream = fn_to_aiter(s.source)
        else:
            raise TypeError(
                f"`source` must be Iterable or AsyncIterable or Callable but got: {s.source}"
            )
        return NoopClosableAsyncIterator(upstream)
