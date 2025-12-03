import asyncio
from inspect import iscoroutinefunction
from typing import (
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
    AFlattenStream,
    AForeachStream,
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
from streamable._utils._func import (
    async_sidify,
    sidify,
    syncify,
)
from streamable._utils._iter import async_to_sync_iter
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
        if (not stream._when or not iscoroutinefunction(stream._when)) and (
            not stream._replace or not iscoroutinefunction(stream._replace)
        ):
            return _functions.catch(
                stream.upstream.accept(cast(IteratorVisitor[Union[T, U]], self)),
                stream._errors,
                when=stream._when,
                replace=cast(Callable[[Exception], U], stream._replace),
                finally_raise=stream._finally_raise,
            )
        elif (not stream._when or iscoroutinefunction(stream._when)) and (
            not stream._replace or iscoroutinefunction(stream._replace)
        ):
            return _functions.acatch(
                self._get_loop(),
                stream.upstream.accept(cast(IteratorVisitor[Union[T, U]], self)),
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

    def visit_distinct_stream(self, stream: DistinctStream[T]) -> Iterator[T]:
        if stream._by is None:
            return _functions.distinct(
                stream.upstream.accept(self),
                stream._by,
                consecutive=stream._consecutive,
            )
        if iscoroutinefunction(stream._by):
            return _functions.adistinct(
                self._get_loop(),
                stream.upstream.accept(self),
                stream._by,
                consecutive=stream._consecutive,
            )
        return _functions.distinct(
            stream.upstream.accept(self),
            stream._by,
            consecutive=stream._consecutive,
        )

    def visit_filter_stream(self, stream: FilterStream[T]) -> Iterator[T]:
        if iscoroutinefunction(stream._where):
            return filter(
                syncify(self._get_loop(), stream._where),
                cast(Iterable[T], stream.upstream.accept(self)),
            )
        return filter(
            stream._where,
            cast(Iterable[T], stream.upstream.accept(self)),
        )

    def visit_flatten_stream(self, stream: FlattenStream[T]) -> Iterator[T]:
        return _functions.flatten(
            stream.upstream.accept(cast(IteratorVisitor[Iterable], self)),
            concurrency=stream._concurrency,
        )

    def visit_aflatten_stream(self, stream: AFlattenStream[T]) -> Iterator[T]:
        return _functions.aflatten(
            self._get_loop(),
            stream.upstream.accept(cast(IteratorVisitor[AsyncIterable], self)),
            concurrency=stream._concurrency,
        )

    def visit_foreach_stream(self, stream: ForeachStream[T]) -> Iterator[T]:
        return self.visit_map_stream(
            MapStream(
                stream.upstream,
                sidify(stream._do),
                stream._concurrency,
                stream._ordered,
                stream._via,
            )
        )

    def visit_aforeach_stream(self, stream: AForeachStream[T]) -> Iterator[T]:
        return self.visit_map_stream(
            MapStream(
                stream.upstream,
                async_sidify(stream._do),
                stream._concurrency,
                stream._ordered,
                "thread",
            )
        )

    def visit_group_stream(self, stream: GroupStream[U]) -> Iterator[T]:
        if iscoroutinefunction(stream._by):
            return cast(
                Iterator[T],
                _functions.agroup(
                    self._get_loop(),
                    stream.upstream.accept(cast(IteratorVisitor[U], self)),
                    stream._size,
                    interval=stream._interval,
                    by=stream._by,
                ),
            )
        return cast(
            Iterator[T],
            _functions.group(
                stream.upstream.accept(cast(IteratorVisitor[U], self)),
                stream._size,
                interval=stream._interval,
                by=stream._by,
            ),
        )

    def visit_groupby_stream(self, stream: GroupbyStream[U, T]) -> Iterator[T]:
        if iscoroutinefunction(stream._key):
            return cast(
                Iterator[T],
                _functions.agroupby(
                    self._get_loop(),
                    stream.upstream.accept(cast(IteratorVisitor[U], self)),
                    stream._key,
                    size=stream._size,
                    interval=stream._interval,
                ),
            )
        return cast(
            Iterator[T],
            _functions.groupby(
                stream.upstream.accept(cast(IteratorVisitor[U], self)),
                stream._key,
                size=stream._size,
                interval=stream._interval,
            ),
        )

    def visit_map_stream(self, stream: MapStream[U, T]) -> Iterator[T]:
        if iscoroutinefunction(stream._to):
            return _functions.amap(
                self._get_loop(),
                stream._to,
                stream.upstream.accept(cast(IteratorVisitor[U], self)),
                concurrency=stream._concurrency,
                ordered=stream._ordered,
            )
        return _functions.map(
            cast(Callable[[U], T], stream._to),
            stream.upstream.accept(cast(IteratorVisitor[U], self)),
            concurrency=stream._concurrency,
            ordered=stream._ordered,
            via=stream._via,
        )

    def visit_observe_stream(self, stream: ObserveStream[T]) -> Iterator[T]:
        return _functions.observe(
            stream.upstream.accept(self),
            stream._what,
        )

    def visit_skip_stream(self, stream: SkipStream[T]) -> Iterator[T]:
        if isinstance(stream._until, int):
            return _functions.skip(
                stream.upstream.accept(self),
                until=stream._until,
            )
        if iscoroutinefunction(stream._until):
            return _functions.askip(
                self._get_loop(),
                stream.upstream.accept(self),
                until=stream._until,
            )
        return _functions.skip(
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
        if isinstance(stream._when, int):
            return _functions.truncate(
                stream.upstream.accept(self),
                when=stream._when,
            )
        if iscoroutinefunction(stream._when):
            return _functions.atruncate(
                self._get_loop(),
                stream.upstream.accept(self),
                when=stream._when,
            )
        return _functions.truncate(
            stream.upstream.accept(self),
            when=stream._when,
        )

    def visit_stream(self, stream: Stream[T]) -> Iterator[T]:
        if isinstance(stream.source, Iterable):
            return stream.source.__iter__()
        if isinstance(stream.source, AsyncIterable):
            return async_to_sync_iter(self._get_loop(), stream.source.__aiter__())

        if callable(stream.source):
            iterable = stream.source()
            if isinstance(iterable, Iterable):
                return iterable.__iter__()
            if isinstance(iterable, AsyncIterable):
                return async_to_sync_iter(self._get_loop(), iterable.__aiter__())
            raise TypeError(
                f"`source` must be an Iterable/AsyncIterable or a Callable[[], Iterable/AsyncIterable] but got a Callable[[], {type(iterable)}]"
            )
        raise TypeError(
            f"`source` must be an Iterable/AsyncIterable or a Callable[[], Iterable/AsyncIterable] but got a {type(stream.source)}"
        )
