from inspect import iscoroutinefunction
from typing import (
    TYPE_CHECKING,
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
from streamable._tools._func import sidify
from streamable._tools._iter import afn_to_aiter, fn_to_aiter, async_iter
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


class AsyncIteratorVisitor(Visitor[AsyncIterator[T]]):
    __slots__ = ()

    def visit_buffer_stream(self, s: "BufferStream[T]") -> AsyncIterator[T]:
        return _afunctions.buffer(
            s.upstream.accept(self),
            s._up_to,
        )

    def visit_catch_stream(self, s: "CatchStream[T, U]") -> AsyncIterator[Union[T, U]]:
        return _afunctions.catch(
            s.upstream.accept(self),
            s._errors,
            where=s._where,
            replace=s._replace,
            do=s._do,
            stop=s._stop,
        )

    def visit_do_stream(self, s: "DoStream[T]") -> AsyncIterator[T]:
        return _afunctions.map(
            sidify(s._effect),
            s.upstream.accept(self),
            concurrency=s._concurrency,
            as_completed=s._as_completed,
        )

    def visit_filter_stream(self, s: "FilterStream[T]") -> AsyncIterator[T]:
        return _afunctions.filter(s._where, s.upstream.accept(self))

    def visit_flatten_stream(self, s: "FlattenStream[T]") -> AsyncIterator[T]:
        return _afunctions.flatten(
            s.upstream.accept(
                cast(AsyncIteratorVisitor[Union[Iterable[T], AsyncIterable[T]]], self)
            ),
            concurrency=s._concurrency,
        )

    def visit_group_stream(self, s: "GroupStream[T]") -> AsyncIterator[T]:
        return cast(
            AsyncIterator[T],
            _afunctions.group(
                s.upstream.accept(self),
                s._up_to,
                within=s._within,
                by=s._by,
            ),
        )

    def visit_map_stream(self, s: "MapStream[U, T]") -> AsyncIterator[T]:
        return _afunctions.map(
            s._into,
            s.upstream.accept(cast(AsyncIteratorVisitor[U], self)),
            concurrency=s._concurrency,
            as_completed=s._as_completed,
        )

    def visit_observe_stream(self, s: "ObserveStream[T]") -> AsyncIterator[T]:
        return _afunctions.observe(
            s.upstream.accept(self),
            s._subject,
            s._every,
            s._do,
        )

    def visit_skip_stream(self, s: "SkipStream[T]") -> AsyncIterator[T]:
        return _afunctions.skip(
            s.upstream.accept(self),
            until=s._until,
        )

    def visit_take_stream(self, s: "TakeStream[T]") -> AsyncIterator[T]:
        return _afunctions.take(
            s.upstream.accept(self),
            until=s._until,
        )

    def visit_throttle_stream(self, s: "ThrottleStream[T]") -> AsyncIterator[T]:
        return _afunctions.throttle(
            s.upstream.accept(self),
            s._up_to,
            per=s._per,
        )

    def visit_stream(self, s: "stream[T]") -> AsyncIterator[T]:
        if isinstance(s.source, (Iterable, AsyncIterable)):
            return async_iter(s.source)
        if callable(s.source):
            if iscoroutinefunction(s.source):
                return afn_to_aiter(
                    cast(Callable[[], Coroutine[Any, Any, T]], s.source)
                )
            else:
                return fn_to_aiter(s.source)
        raise TypeError(
            f"`source` must be Iterable or AsyncIterable or Callable but got: {s.source}"
        )
