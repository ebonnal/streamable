import asyncio
from typing import AsyncIterable, Iterable, Iterator, Optional, TypeVar, cast

from streamable import functions
from streamable.stream import (
    ACatchStream,
    ADistinctStream,
    AFilterStream,
    AFlattenStream,
    AForeachStream,
    AGroupbyStream,
    AGroupStream,
    AMapStream,
    ASkipStream,
    ATruncateStream,
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
from streamable.util.functiontools import (
    async_sidify,
    sidify,
    syncify,
)
from streamable.util.iterabletools import IteratorWithClosingLoop, async_to_sync_iter
from streamable.visitors import Visitor

T = TypeVar("T")
U = TypeVar("U")


class IteratorVisitor(Visitor[Iterator[T]]):
    def __init__(self) -> None:
        # will only be set by `_get_event_loop` if an operation needs it
        self.event_loop: Optional[asyncio.AbstractEventLoop] = None

    def _get_event_loop(self) -> asyncio.AbstractEventLoop:
        if not self.event_loop:
            self.event_loop = asyncio.new_event_loop()
        return self.event_loop

    def visit_catch_stream(self, stream: CatchStream[T]) -> Iterator[T]:
        return functions.catch(
            stream.upstream.accept(self),
            stream._errors,
            when=stream._when,
            replacement=stream._replacement,
            finally_raise=stream._finally_raise,
        )

    def visit_acatch_stream(self, stream: ACatchStream[T]) -> Iterator[T]:
        return IteratorWithClosingLoop(
            functions.acatch(
                self._get_event_loop(),
                stream.upstream.accept(self),
                stream._errors,
                when=stream._when,
                replacement=stream._replacement,
                finally_raise=stream._finally_raise,
            ),
            self._get_event_loop(),
        )

    def visit_distinct_stream(self, stream: DistinctStream[T]) -> Iterator[T]:
        return functions.distinct(
            stream.upstream.accept(self),
            stream._key,
            consecutive_only=stream._consecutive_only,
        )

    def visit_adistinct_stream(self, stream: ADistinctStream[T]) -> Iterator[T]:
        return IteratorWithClosingLoop(
            functions.adistinct(
                self._get_event_loop(),
                stream.upstream.accept(self),
                stream._key,
                consecutive_only=stream._consecutive_only,
            ),
            self._get_event_loop(),
        )

    def visit_filter_stream(self, stream: FilterStream[T]) -> Iterator[T]:
        return filter(
            stream._when,
            cast(Iterable[T], stream.upstream.accept(self)),
        )

    def visit_afilter_stream(self, stream: AFilterStream[T]) -> Iterator[T]:
        return IteratorWithClosingLoop(
            filter(
                syncify(self._get_event_loop(), stream._when),
                cast(Iterable[T], stream.upstream.accept(self)),
            ),
            self._get_event_loop(),
        )

    def visit_flatten_stream(self, stream: FlattenStream[T]) -> Iterator[T]:
        return functions.flatten(
            stream.upstream.accept(cast(IteratorVisitor[Iterable], self)),
            concurrency=stream._concurrency,
        )

    def visit_aflatten_stream(self, stream: AFlattenStream[T]) -> Iterator[T]:
        return IteratorWithClosingLoop(
            functions.aflatten(
                self._get_event_loop(),
                stream.upstream.accept(cast(IteratorVisitor[AsyncIterable], self)),
                concurrency=stream._concurrency,
            ),
            self._get_event_loop(),
        )

    def visit_foreach_stream(self, stream: ForeachStream[T]) -> Iterator[T]:
        return self.visit_map_stream(
            MapStream(
                stream.upstream,
                sidify(stream._effect),
                stream._concurrency,
                stream._ordered,
                stream._via,
            )
        )

    def visit_aforeach_stream(self, stream: AForeachStream[T]) -> Iterator[T]:
        return self.visit_amap_stream(
            AMapStream(
                stream.upstream,
                async_sidify(stream._effect),
                stream._concurrency,
                stream._ordered,
            )
        )

    def visit_group_stream(self, stream: GroupStream[U]) -> Iterator[T]:
        return cast(
            Iterator[T],
            functions.group(
                stream.upstream.accept(cast(IteratorVisitor[U], self)),
                stream._size,
                interval=stream._interval,
                by=stream._by,
            ),
        )

    def visit_agroup_stream(self, stream: AGroupStream[U]) -> Iterator[T]:
        return IteratorWithClosingLoop(
            cast(
                Iterator[T],
                functions.agroup(
                    self._get_event_loop(),
                    stream.upstream.accept(cast(IteratorVisitor[U], self)),
                    stream._size,
                    interval=stream._interval,
                    by=stream._by,
                ),
            ),
            self._get_event_loop(),
        )

    def visit_groupby_stream(self, stream: GroupbyStream[U, T]) -> Iterator[T]:
        return cast(
            Iterator[T],
            functions.groupby(
                stream.upstream.accept(cast(IteratorVisitor[U], self)),
                stream._key,
                size=stream._size,
                interval=stream._interval,
            ),
        )

    def visit_agroupby_stream(self, stream: AGroupbyStream[U, T]) -> Iterator[T]:
        return IteratorWithClosingLoop(
            cast(
                Iterator[T],
                functions.agroupby(
                    self._get_event_loop(),
                    stream.upstream.accept(cast(IteratorVisitor[U], self)),
                    stream._key,
                    size=stream._size,
                    interval=stream._interval,
                ),
            ),
            self._get_event_loop(),
        )

    def visit_map_stream(self, stream: MapStream[U, T]) -> Iterator[T]:
        return functions.map(
            stream._transformation,
            stream.upstream.accept(cast(IteratorVisitor[U], self)),
            concurrency=stream._concurrency,
            ordered=stream._ordered,
            via=stream._via,
        )

    def visit_amap_stream(self, stream: AMapStream[U, T]) -> Iterator[T]:
        return IteratorWithClosingLoop(
            functions.amap(
                self._get_event_loop(),
                stream._transformation,
                stream.upstream.accept(cast(IteratorVisitor[U], self)),
                concurrency=stream._concurrency,
                ordered=stream._ordered,
            ),
            self._get_event_loop(),
        )

    def visit_observe_stream(self, stream: ObserveStream[T]) -> Iterator[T]:
        return functions.observe(
            stream.upstream.accept(self),
            stream._what,
        )

    def visit_skip_stream(self, stream: SkipStream[T]) -> Iterator[T]:
        return functions.skip(
            stream.upstream.accept(self),
            stream._count,
            until=stream._until,
        )

    def visit_askip_stream(self, stream: ASkipStream[T]) -> Iterator[T]:
        return IteratorWithClosingLoop(
            functions.askip(
                self._get_event_loop(),
                stream.upstream.accept(self),
                stream._count,
                until=stream._until,
            ),
            self._get_event_loop(),
        )

    def visit_throttle_stream(self, stream: ThrottleStream[T]) -> Iterator[T]:
        return functions.throttle(
            stream.upstream.accept(self),
            stream._count,
            per=stream._per,
        )

    def visit_truncate_stream(self, stream: TruncateStream[T]) -> Iterator[T]:
        return functions.truncate(
            stream.upstream.accept(self),
            stream._count,
            when=stream._when,
        )

    def visit_atruncate_stream(self, stream: ATruncateStream[T]) -> Iterator[T]:
        return IteratorWithClosingLoop(
            functions.atruncate(
                self._get_event_loop(),
                stream.upstream.accept(self),
                stream._count,
                when=stream._when,
            ),
            self._get_event_loop(),
        )

    def visit_stream(self, stream: Stream[T]) -> Iterator[T]:
        if isinstance(stream.source, Iterable):
            return stream.source.__iter__()
        if isinstance(stream.source, AsyncIterable):
            return IteratorWithClosingLoop(
                async_to_sync_iter(self._get_event_loop(), stream.source),
                self._get_event_loop(),
            )
        if callable(stream.source):
            iterable = stream.source()
            if isinstance(iterable, Iterable):
                return iterable.__iter__()
            if isinstance(iterable, AsyncIterable):
                return IteratorWithClosingLoop(
                    async_to_sync_iter(self._get_event_loop(), iterable),
                    self._get_event_loop(),
                )
            raise TypeError(
                f"`source` must be an Iterable/AsyncIterable or a Callable[[], Iterable/AsyncIterable] but got a Callable[[], {type(iterable)}]"
            )
        raise TypeError(
            f"`source` must be an Iterable/AsyncIterable or a Callable[[], Iterable/AsyncIterable] but got a {type(stream.source)}"
        )
