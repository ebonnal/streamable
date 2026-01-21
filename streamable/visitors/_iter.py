import asyncio
from inspect import iscoroutinefunction
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    Callable,
    Coroutine,
    Iterable,
    Iterator,
    Optional,
    TypeVar,
    Union,
    cast,
)

from streamable import _functions

from streamable._tools._func import sidify
from streamable._tools._iter import afn_to_iter, fn_to_iter, sync_iter
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


class IteratorVisitor(Visitor[Iterator[T]]):
    __slots__ = ("loop",)

    def __init__(self) -> None:
        # will only be set by `_get_loop` if an operation needs it
        self.loop: Optional[asyncio.AbstractEventLoop] = None

    def _lazy_loop(self) -> asyncio.AbstractEventLoop:
        if not self.loop:
            self.loop = asyncio.new_event_loop()
        return self.loop

    def visit_buffer_stream(self, s: "BufferStream[T]") -> Iterator[T]:
        return _functions.buffer(
            s.upstream.accept(self),
            s._up_to,
        )

    def visit_catch_stream(self, s: "CatchStream[T, U]") -> Iterator[Union[T, U]]:
        return _functions.catch(
            self._lazy_loop,
            s.upstream.accept(self),
            s._errors,
            where=s._where,
            replace=s._replace,
            do=s._do,
            stop=s._stop,
        )

    def visit_filter_stream(self, s: "FilterStream[T]") -> Iterator[T]:
        return _functions.filter(
            self._lazy_loop,
            s._where,
            s.upstream.accept(self),
        )

    def visit_flatten_stream(self, s: "FlattenStream[T]") -> Iterator[T]:
        return _functions.flatten(
            self._lazy_loop,
            s.upstream.accept(
                cast(IteratorVisitor[Union[Iterable[T], AsyncIterable[T]]], self)
            ),
            concurrency=s._concurrency,
        )

    def visit_do_stream(self, s: "DoStream[T]") -> Iterator[T]:
        return _functions.map(
            self._lazy_loop,
            sidify(s._effect),
            s.upstream.accept(self),
            concurrency=s._concurrency,
            as_completed=s._as_completed,
        )

    def visit_group_stream(self, s: "GroupStream[T]") -> Iterator[T]:
        return cast(
            Iterator[T],
            _functions.group(
                self._lazy_loop,
                s.upstream.accept(self),
                s._up_to,
                every=s._every,
                by=s._by,
            ),
        )

    def visit_map_stream(self, s: "MapStream[U, T]") -> Iterator[T]:
        return _functions.map(
            self._lazy_loop,
            s._into,
            s.upstream.accept(cast(IteratorVisitor[U], self)),
            concurrency=s._concurrency,
            as_completed=s._as_completed,
        )

    def visit_observe_stream(self, s: "ObserveStream[T]") -> Iterator[T]:
        return _functions.observe(
            self._lazy_loop,
            s.upstream.accept(self),
            s._subject,
            s._every,
            s._do,
        )

    def visit_skip_stream(self, s: "SkipStream[T]") -> Iterator[T]:
        return _functions.skip(
            self._lazy_loop,
            s.upstream.accept(self),
            until=s._until,
        )

    def visit_take_stream(self, s: "TakeStream[T]") -> Iterator[T]:
        return _functions.take(
            self._lazy_loop,
            s.upstream.accept(self),
            until=s._until,
        )

    def visit_throttle_stream(self, s: "ThrottleStream[T]") -> Iterator[T]:
        return _functions.throttle(
            s.upstream.accept(self),
            s._up_to,
            per=s._per,
        )

    def visit_stream(self, s: "stream[T]") -> Iterator[T]:
        if isinstance(s.source, (Iterable, AsyncIterable)):
            return sync_iter(self._lazy_loop, s.source)
        if callable(s.source):
            if iscoroutinefunction(s.source):
                return afn_to_iter(
                    self._lazy_loop(),
                    cast(Callable[[], Coroutine[Any, Any, T]], s.source),
                )
            else:
                return fn_to_iter(s.source)
        raise TypeError(
            f"`source` must be Iterable or AsyncIterable or Callable but got {s.source}"
        )
