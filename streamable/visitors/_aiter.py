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
from streamable._utils._func import sidify
from streamable._utils._iter import afn_to_aiter, fn_to_aiter, async_iter
from streamable.visitors import Visitor

if TYPE_CHECKING:  # pragma: no cover
    from streamable._stream import (
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
    def visit_catch_stream(
        self, stream: "CatchStream[T, U]"
    ) -> AsyncIterator[Union[T, U]]:
        return _afunctions.catch(
            stream.upstream.accept(self),
            stream._errors,
            where=stream._where,
            replace=stream._replace,
            do=stream._do,
            stop=stream._stop,
        )

    def visit_filter_stream(self, stream: "FilterStream[T]") -> AsyncIterator[T]:
        return _afunctions.filter(stream._where, stream.upstream.accept(self))

    def visit_flatten_stream(self, stream: "FlattenStream[T]") -> AsyncIterator[T]:
        return _afunctions.flatten(
            stream.upstream.accept(
                cast(AsyncIteratorVisitor[Union[Iterable[T], AsyncIterable[T]]], self)
            ),
            concurrency=stream._concurrency,
        )

    def visit_do_stream(self, stream: "DoStream[T]") -> AsyncIterator[T]:
        return _afunctions.map(
            sidify(stream._effect),
            stream.upstream.accept(self),
            concurrency=stream._concurrency,
            ordered=stream._ordered,
        )

    def visit_group_stream(self, stream: "GroupStream[T]") -> AsyncIterator[T]:
        return cast(
            AsyncIterator[T],
            _afunctions.group(
                stream.upstream.accept(self),
                stream._up_to,
                every=stream._every,
                by=stream._by,
            ),
        )

    def visit_map_stream(self, stream: "MapStream[U, T]") -> AsyncIterator[T]:
        return _afunctions.map(
            stream._into,
            stream.upstream.accept(cast(AsyncIteratorVisitor[U], self)),
            concurrency=stream._concurrency,
            ordered=stream._ordered,
        )

    def visit_observe_stream(self, stream: "ObserveStream[T]") -> AsyncIterator[T]:
        return _afunctions.observe(
            stream.upstream.accept(self),
            stream._subject,
            stream._every,
            stream._how,
        )

    def visit_skip_stream(self, stream: "SkipStream[T]") -> AsyncIterator[T]:
        return _afunctions.skip(
            stream.upstream.accept(self),
            until=stream._until,
        )

    def visit_take_stream(self, stream: "TakeStream[T]") -> AsyncIterator[T]:
        return _afunctions.take(
            stream.upstream.accept(self),
            until=stream._until,
        )

    def visit_throttle_stream(self, stream: "ThrottleStream[T]") -> AsyncIterator[T]:
        return _afunctions.throttle(
            stream.upstream.accept(self),
            stream._up_to,
            per=stream._per,
        )

    def visit_stream(self, stream: "stream[T]") -> AsyncIterator[T]:
        if isinstance(stream.source, (Iterable, AsyncIterable)):
            return async_iter(stream.source)
        if callable(stream.source):
            if iscoroutinefunction(stream.source):
                return afn_to_aiter(
                    cast(Callable[[], Coroutine[Any, Any, T]], stream.source)
                )
            else:
                return fn_to_aiter(stream.source)
        raise TypeError(
            f"`source` must be Iterable or AsyncIterable or Callable but got {stream.source}"
        )
