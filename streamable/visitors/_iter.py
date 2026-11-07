from typing import (
    TYPE_CHECKING,
    AsyncIterable,
    Callable,
    Iterable,
    TypeVar,
    Union,
    cast,
)

from streamable import _functions
from streamable._tools._func import sidify
from streamable._tools._iter import ClosableIterator, GuardedIterator, fn_to_iter
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
        TakeStream,
        ThrottleStream,
        stream,
    )

T = TypeVar("T")
U = TypeVar("U")


class IteratorVisitor(Visitor[ClosableIterator[T]]):
    __slots__ = ()

    def visit_buffer_stream(self, s: "BufferStream[T]") -> ClosableIterator[T]:
        return _functions.buffer(
            s.upstream.accept(self),
            s._up_to,
        )

    def visit_catch_stream(
        self, s: "CatchStream[T, U]"
    ) -> ClosableIterator[Union[T, U]]:
        return _functions.catch(
            s.upstream.accept(self),
            s._errors,
            where=s._where,
            replace=cast(Callable[[Exception], U], s._replace),
            do=s._do,
            stop=s._stop,
        )

    def visit_do_stream(self, s: "DoStream[T]") -> ClosableIterator[T]:
        return _functions.map(
            sidify(s._effect),
            s.upstream.accept(self),
            concurrency=s._concurrency,
            as_completed=s._as_completed,
        )

    def visit_filter_stream(self, s: "FilterStream[T]") -> ClosableIterator[T]:
        return _functions.filter(
            s._where,
            s.upstream.accept(self),
        )

    def visit_flatten_stream(self, s: "FlattenStream[T]") -> ClosableIterator[T]:
        return _functions.flatten(
            s.upstream.accept(
                cast(IteratorVisitor[Union[Iterable[T], AsyncIterable[T]]], self)
            ),
            concurrency=s._concurrency,
        )

    def visit_group_stream(self, s: "GroupStream[T]") -> ClosableIterator[T]:
        return cast(
            ClosableIterator[T],
            _functions.group(
                s.upstream.accept(self),
                s._up_to,
                within=s._within,
                by=s._by,
            ),
        )

    def visit_map_stream(self, s: "MapStream[U, T]") -> ClosableIterator[T]:
        return _functions.map(
            cast(Callable[[U], T], s._into),
            s.upstream.accept(cast(IteratorVisitor[U], self)),
            concurrency=s._concurrency,
            as_completed=s._as_completed,
        )

    def visit_observe_stream(self, s: "ObserveStream[T]") -> ClosableIterator[T]:
        return _functions.observe(
            s.upstream.accept(self),
            s._subject,
            s._every,
            s._do,
        )

    def visit_skip_stream(self, s: "SkipStream[T]") -> ClosableIterator[T]:
        return _functions.skip(
            s.upstream.accept(self),
            until=s._until,
        )

    def visit_take_stream(self, s: "TakeStream[T]") -> ClosableIterator[T]:
        return _functions.take(
            s.upstream.accept(self),
            until=s._until,
        )

    def visit_throttle_stream(self, s: "ThrottleStream[T]") -> ClosableIterator[T]:
        return _functions.throttle(
            s.upstream.accept(self),
            s._up_to,
            per=s._per,
        )

    def visit_stream(self, s: "stream[T]") -> ClosableIterator[T]:
        if isinstance(s.source, Iterable):
            return GuardedIterator(s.source)
        if callable(s.source):
            return GuardedIterator(fn_to_iter(s.source))
        raise TypeError(
            f"`source` must be Iterable or AsyncIterable or Callable but got: {s.source}"
        )
