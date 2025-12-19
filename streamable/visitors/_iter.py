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

from streamable._utils._func import sidify
from streamable._utils._iter import afn_to_iter, async_to_sync_iter, fn_to_iter
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


class IteratorVisitor(Visitor[Iterator[T]]):
    def __init__(self) -> None:
        # will only be set by `_get_loop` if an operation needs it
        self.loop: Optional[asyncio.AbstractEventLoop] = None

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        if not self.loop:
            self.loop = asyncio.new_event_loop()
        return self.loop

    def visit_catch_stream(self, stream: "CatchStream[T, U]") -> Iterator[Union[T, U]]:
        return _functions.catch(
            self._get_loop,
            stream.upstream.accept(self),
            stream._errors,
            where=stream._where,
            replace=stream._replace,
            do=stream._do,
            stop=stream._stop,
        )

    def visit_filter_stream(self, stream: "FilterStream[T]") -> Iterator[T]:
        return _functions.filter(
            self._get_loop,
            stream._where,
            stream.upstream.accept(self),
        )

    def visit_flatten_stream(self, stream: "FlattenStream[T]") -> Iterator[T]:
        return _functions.flatten(
            self._get_loop,
            stream.upstream.accept(
                cast(IteratorVisitor[Union[Iterable[T], AsyncIterable[T]]], self)
            ),
            concurrency=stream._concurrency,
        )

    def visit_do_stream(self, stream: "DoStream[T]") -> Iterator[T]:
        return _functions.map(
            self._get_loop,
            sidify(stream._effect),
            stream.upstream.accept(self),
            concurrency=stream._concurrency,
            ordered=stream._ordered,
        )

    def visit_group_stream(self, stream: "GroupStream[T]") -> Iterator[T]:
        return cast(
            Iterator[T],
            _functions.group(
                self._get_loop,
                stream.upstream.accept(self),
                stream._up_to,
                every=stream._every,
                by=stream._by,
            ),
        )

    def visit_map_stream(self, stream: "MapStream[U, T]") -> Iterator[T]:
        return _functions.map(
            self._get_loop,
            stream._into,
            stream.upstream.accept(cast(IteratorVisitor[U], self)),
            concurrency=stream._concurrency,
            ordered=stream._ordered,
        )

    def visit_observe_stream(self, stream: "ObserveStream[T]") -> Iterator[T]:
        return _functions.observe(
            stream.upstream.accept(self),
            stream._subject,
            stream._every,
            stream._how,
        )

    def visit_skip_stream(self, stream: "SkipStream[T]") -> Iterator[T]:
        return _functions.skip(
            self._get_loop,
            stream.upstream.accept(self),
            until=stream._until,
        )

    def visit_take_stream(self, stream: "TakeStream[T]") -> Iterator[T]:
        return _functions.take(
            self._get_loop,
            stream.upstream.accept(self),
            until=stream._until,
        )

    def visit_throttle_stream(self, stream: "ThrottleStream[T]") -> Iterator[T]:
        return _functions.throttle(
            stream.upstream.accept(self),
            stream._up_to,
            per=stream._per,
        )

    def visit_stream(self, stream: "stream[T]") -> Iterator[T]:
        if isinstance(stream.source, Iterable):
            return stream.source.__iter__()
        if isinstance(stream.source, AsyncIterable):
            return async_to_sync_iter(self._get_loop(), stream.source.__aiter__())

        if callable(stream.source):
            if iscoroutinefunction(stream.source):
                return afn_to_iter(
                    self._get_loop(),
                    cast(Callable[[], Coroutine[Any, Any, T]], stream.source),
                )
            else:
                return fn_to_iter(stream.source)
        raise TypeError(
            f"`source` must be Iterable or AsyncIterable or Callable but got {stream.source}"
        )
