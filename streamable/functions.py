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

from streamable.iterators import (
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
from streamable.util.constants import NO_REPLACEMENT
from streamable.util.functiontools import syncify

with suppress(ImportError):
    from typing import Literal

T = TypeVar("T")
U = TypeVar("U")


def catch(
    iterator: Iterator[T],
    errors: Union[
        Optional[Type[Exception]], Iterable[Optional[Type[Exception]]]
    ] = Exception,
    *,
    when: Optional[Callable[[Exception], Any]] = None,
    replacement: T = NO_REPLACEMENT,  # type: ignore
    finally_raise: bool = False,
) -> Iterator[T]:
    if errors is None:
        return iterator
    return CatchIterator(
        iterator,
        tuple(filter(None, errors)) if isinstance(errors, Iterable) else errors,
        when=when,
        replacement=replacement,
        finally_raise=finally_raise,
    )


def acatch(
    event_loop: asyncio.AbstractEventLoop,
    iterator: Iterator[T],
    errors: Union[
        Optional[Type[Exception]], Iterable[Optional[Type[Exception]]]
    ] = Exception,
    *,
    when: Optional[Callable[[Exception], Coroutine[Any, Any, Any]]] = None,
    replacement: T = NO_REPLACEMENT,  # type: ignore
    finally_raise: bool = False,
) -> Iterator[T]:
    return catch(
        iterator,
        errors,
        when=syncify(event_loop, when) if when else None,
        replacement=replacement,
        finally_raise=finally_raise,
    )


def distinct(
    iterator: Iterator[T],
    key: Optional[Callable[[T], Any]] = None,
    *,
    consecutive_only: bool = False,
) -> Iterator[T]:
    if consecutive_only:
        return ConsecutiveDistinctIterator(iterator, key)
    return DistinctIterator(iterator, key)


def adistinct(
    event_loop: asyncio.AbstractEventLoop,
    iterator: Iterator[T],
    key: Optional[Callable[[T], Any]] = None,
    *,
    consecutive_only: bool = False,
) -> Iterator[T]:
    return distinct(
        iterator,
        syncify(event_loop, key) if key else None,
        consecutive_only=consecutive_only,
    )


def flatten(iterator: Iterator[Iterable[T]], *, concurrency: int = 1) -> Iterator[T]:
    if concurrency == 1:
        return FlattenIterator(iterator)
    else:
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
    else:
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
    transformation: Callable[[T], U],
    iterator: Iterator[T],
    *,
    concurrency: int = 1,
    ordered: bool = True,
    via: "Literal['thread', 'process']" = "thread",
) -> Iterator[U]:
    if concurrency == 1:
        return builtins.map(transformation, iterator)
    else:
        return ConcurrentMapIterator(
            iterator,
            transformation,
            concurrency=concurrency,
            buffersize=concurrency,
            ordered=ordered,
            via=via,
        )


def amap(
    event_loop: asyncio.AbstractEventLoop,
    transformation: Callable[[T], Coroutine[Any, Any, U]],
    iterator: Iterator[T],
    *,
    concurrency: int = 1,
    ordered: bool = True,
) -> Iterator[U]:
    if concurrency == 1:
        return map(syncify(event_loop, transformation), iterator)
    return ConcurrentAMapIterator(
        event_loop,
        iterator,
        transformation,
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
