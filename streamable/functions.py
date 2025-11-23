import asyncio
import builtins
import datetime
from contextlib import suppress
from operator import itemgetter
from typing import (
    Any,
    AsyncIterable,
    Callable,
    Coroutine,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from streamable._iterators import (
    AFlattenIterator,
    CatchIterator,
    ConcurrentAFlattenIterator,
    ConcurrentAMapIterator,
    ConcurrentFlattenIterator,
    ConcurrentMapIterator,
    ConsecutiveDistinctIterator,
    CountAndPredicateSkipIterator,
    CountSkipIterator,
    CountTruncateIterator,
    DistinctIterator,
    FlattenIterator,
    GroupbyIterator,
    GroupIterator,
    ObserveIterator,
    PredicateSkipIterator,
    PredicateTruncateIterator,
    YieldsPerPeriodThrottleIterator,
)
from streamable._utils._func import syncify

with suppress(ImportError):
    from typing import Literal

T = TypeVar("T")
U = TypeVar("U")


def catch(
    iterator: Iterator[T],
    errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
    *,
    when: Optional[Callable[[Exception], Any]] = None,
    replace: Optional[Callable[[Exception], U]] = None,
    finally_raise: bool = False,
) -> Iterator[Union[T, U]]:
    return CatchIterator(
        iterator,
        errors,
        when=when,
        replace=replace,
        finally_raise=finally_raise,
    )


def acatch(
    event_loop: asyncio.AbstractEventLoop,
    iterator: Iterator[T],
    errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
    *,
    when: Optional[Callable[[Exception], Coroutine[Any, Any, Any]]] = None,
    replace: Optional[Callable[[Exception], Coroutine[Any, Any, U]]] = None,
    finally_raise: bool = False,
) -> Iterator[Union[T, U]]:
    return catch(
        iterator,
        errors,
        when=syncify(event_loop, when) if when else None,
        replace=syncify(event_loop, replace) if replace else None,
        finally_raise=finally_raise,
    )


def distinct(
    iterator: Iterator[T],
    by: Optional[Callable[[T], Any]] = None,
    *,
    consecutive: bool = False,
) -> Iterator[T]:
    if consecutive:
        return ConsecutiveDistinctIterator(iterator, by)
    return DistinctIterator(iterator, by)


def adistinct(
    event_loop: asyncio.AbstractEventLoop,
    iterator: Iterator[T],
    by: Optional[Callable[[T], Any]] = None,
    *,
    consecutive: bool = False,
) -> Iterator[T]:
    return distinct(
        iterator,
        syncify(event_loop, by) if by else None,
        consecutive=consecutive,
    )


def flatten(iterator: Iterator[Iterable[T]], *, concurrency: int = 1) -> Iterator[T]:
    if concurrency == 1:
        return FlattenIterator(iterator)
    return ConcurrentFlattenIterator(
        iterator,
        concurrency=concurrency,
        buffersize=concurrency,
    )


def aflatten(
    event_loop: asyncio.AbstractEventLoop,
    iterator: Iterator[AsyncIterable[T]],
    *,
    concurrency: int = 1,
) -> Iterator[T]:
    if concurrency == 1:
        return AFlattenIterator(event_loop, iterator)
    return ConcurrentAFlattenIterator(
        event_loop,
        iterator,
        concurrency=concurrency,
        buffersize=concurrency,
    )


def group(
    iterator: Iterator[T],
    size: Optional[int] = None,
    *,
    interval: Optional[datetime.timedelta] = None,
    by: Optional[Callable[[T], Any]] = None,
) -> Iterator[List[T]]:
    if by is None:
        return GroupIterator(iterator, size, interval)
    return map(itemgetter(1), GroupbyIterator(iterator, by, size, interval))


def agroup(
    event_loop: asyncio.AbstractEventLoop,
    iterator: Iterator[T],
    size: Optional[int] = None,
    *,
    interval: Optional[datetime.timedelta] = None,
    by: Optional[Callable[[T], Coroutine[Any, Any, Any]]] = None,
) -> Iterator[List[T]]:
    return group(
        iterator,
        size,
        interval=interval,
        by=syncify(event_loop, by) if by else None,
    )


def groupby(
    iterator: Iterator[T],
    key: Callable[[T], U],
    *,
    size: Optional[int] = None,
    interval: Optional[datetime.timedelta] = None,
) -> Iterator[Tuple[U, List[T]]]:
    return GroupbyIterator(iterator, key, size, interval)


def agroupby(
    event_loop: asyncio.AbstractEventLoop,
    iterator: Iterator[T],
    key: Callable[[T], Coroutine[Any, Any, U]],
    *,
    size: Optional[int] = None,
    interval: Optional[datetime.timedelta] = None,
) -> Iterator[Tuple[U, List[T]]]:
    return groupby(
        iterator,
        syncify(event_loop, key),
        size=size,
        interval=interval,
    )


def map(
    to: Callable[[T], U],
    iterator: Iterator[T],
    *,
    concurrency: int = 1,
    ordered: bool = True,
    via: "Literal['thread', 'process']" = "thread",
) -> Iterator[U]:
    if concurrency == 1:
        return builtins.map(to, iterator)
    return ConcurrentMapIterator(
        iterator,
        to,
        concurrency=concurrency,
        buffersize=concurrency,
        ordered=ordered,
        via=via,
    )


def amap(
    event_loop: asyncio.AbstractEventLoop,
    to: Callable[[T], Coroutine[Any, Any, U]],
    iterator: Iterator[T],
    *,
    concurrency: int = 1,
    ordered: bool = True,
) -> Iterator[U]:
    if concurrency == 1:
        return map(syncify(event_loop, to), iterator)
    return ConcurrentAMapIterator(
        event_loop,
        iterator,
        to,
        concurrency=concurrency,
        buffersize=concurrency,
        ordered=ordered,
    )


def observe(iterator: Iterator[T], what: str) -> Iterator[T]:
    return ObserveIterator(iterator, what)


def skip(
    iterator: Iterator[T],
    count: Optional[int] = None,
    *,
    until: Optional[Callable[[T], Any]] = None,
) -> Iterator[T]:
    if until is not None:
        if count is not None:
            return CountAndPredicateSkipIterator(iterator, count, until)
        return PredicateSkipIterator(iterator, until)
    if count is not None:
        return CountSkipIterator(iterator, count)
    return iterator


def askip(
    event_loop: asyncio.AbstractEventLoop,
    iterator: Iterator[T],
    count: Optional[int] = None,
    *,
    until: Optional[Callable[[T], Coroutine[Any, Any, Any]]] = None,
) -> Iterator[T]:
    return skip(
        iterator,
        count,
        until=syncify(event_loop, until) if until else None,
    )


def throttle(
    iterator: Iterator[T],
    count: Optional[int],
    *,
    per: Optional[datetime.timedelta] = None,
) -> Iterator[T]:
    if count and per:
        iterator = YieldsPerPeriodThrottleIterator(iterator, count, per)
    return iterator


def truncate(
    iterator: Iterator[T],
    count: Optional[int] = None,
    *,
    when: Optional[Callable[[T], Any]] = None,
) -> Iterator[T]:
    if count is not None:
        iterator = CountTruncateIterator(iterator, count)
    if when is not None:
        iterator = PredicateTruncateIterator(iterator, when)
    return iterator


def atruncate(
    event_loop: asyncio.AbstractEventLoop,
    iterator: Iterator[T],
    count: Optional[int] = None,
    *,
    when: Optional[Callable[[T], Coroutine[Any, Any, Any]]] = None,
) -> Iterator[T]:
    return truncate(
        iterator,
        count,
        when=syncify(event_loop, when) if when else None,
    )
