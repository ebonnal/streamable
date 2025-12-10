import asyncio
from inspect import iscoroutinefunction
from typing import (
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
from streamable._utils._func import (
    async_sidify,
    sidify,
)
from streamable._utils._iter import afn_to_iter, async_to_sync_iter, fn_to_iter
from streamable.visitors import Visitor

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

    def visit_catch_stream(self, stream: CatchStream[T, U]) -> Iterator[Union[T, U]]:
        return _functions.catch(
            self._get_loop,
            stream.upstream.accept(cast(IteratorVisitor[Union[T, U]], self)),
            stream._errors,
            when=stream._when,
            replace=stream._replace,
            do=stream._do,
            finally_raise=stream._finally_raise,
            terminate=stream._terminate,
        )

    def visit_filter_stream(self, stream: FilterStream[T]) -> Iterator[T]:
        return _functions.filter(
            self._get_loop,
            stream._where,
            stream.upstream.accept(self),
        )

    def visit_flatten_stream(self, stream: FlattenStream[T]) -> Iterator[T]:
        return _functions.flatten(
            self._get_loop,
            stream.upstream.accept(
                cast(IteratorVisitor[Union[Iterable[T], AsyncIterable[T]]], self)
            ),
            concurrency=stream._concurrency,
        )

    def visit_do_stream(self, stream: DoStream[T]) -> Iterator[T]:
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

    def visit_group_stream(self, stream: GroupStream[U]) -> Iterator[T]:
        return cast(
            Iterator[T],
            _functions.group(
                self._get_loop,
                stream.upstream.accept(cast(IteratorVisitor[U], self)),
                stream._up_to,
                over=stream._over,
                by=stream._by,
            ),
        )

    def visit_groupby_stream(self, stream: GroupbyStream[U, T]) -> Iterator[T]:
        return cast(
            Iterator[T],
            _functions.groupby(
                self._get_loop,
                stream.upstream.accept(cast(IteratorVisitor[U], self)),
                stream._by,
                up_to=stream._up_to,
                over=stream._over,
            ),
        )

    def visit_map_stream(self, stream: MapStream[U, T]) -> Iterator[T]:
        return _functions.map(
            self._get_loop,
            cast(Callable[[U], T], stream._into),
            stream.upstream.accept(cast(IteratorVisitor[U], self)),
            concurrency=stream._concurrency,
            ordered=stream._ordered,
        )

    def visit_observe_stream(self, stream: ObserveStream[T]) -> Iterator[T]:
        return _functions.observe(
            stream.upstream.accept(self),
            stream._label,
            stream._every,
        )

    def visit_skip_stream(self, stream: SkipStream[T]) -> Iterator[T]:
        return _functions.skip(
            self._get_loop,
            stream.upstream.accept(self),
            until=stream._until,
        )

    def visit_throttle_stream(self, stream: ThrottleStream[T]) -> Iterator[T]:
        return _functions.throttle(
            stream.upstream.accept(self),
            stream._up_to,
            per=stream._per,
        )

    def visit_truncate_stream(self, stream: TruncateStream[T]) -> Iterator[T]:
        return _functions.truncate(
            self._get_loop,
            stream.upstream.accept(self),
            when=stream._when,
        )

    def visit_stream(self, stream: stream[T]) -> Iterator[T]:
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
            f"`source` must be Iterable or AsyncIterable or Callable but got {type(stream.source)}"
        )
