import asyncio
import builtins
from concurrent.futures import Executor
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
    cast,
)

from streamable._iterators import (
    CatchIterator,
    ConcurrentAMapIterator,
    ConcurrentFlattenIterator,
    ConcurrentMapIterator,
    CountSkipIterator,
    CountTruncateIterator,
    EveryIntObserveIterator,
    EveryIntervalObserveIterator,
    FlattenIterator,
    GroupbyIterator,
    GroupIterator,
    PowerObserveIterator,
    PredicateSkipIterator,
    PredicateTruncateIterator,
    YieldsPerPeriodThrottleIterator,
)
from streamable._utils._func import syncify

with suppress(ImportError):
    pass

T = TypeVar("T")
U = TypeVar("U")


def catch(
    iterator: Iterator[T],
    errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
    *,
    when: Optional[Callable[[Exception], Any]] = None,
    replace: Optional[Callable[[Exception], U]] = None,
    do: Optional[Callable[[Exception], Any]] = None,
    finally_raise: bool = False,
) -> Iterator[Union[T, U]]:
    return CatchIterator(
        iterator,
        errors,
        when=when,
        replace=replace,
        do=do,
        finally_raise=finally_raise,
    )


def acatch(
    loop: asyncio.AbstractEventLoop,
    iterator: Iterator[T],
    errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
    *,
    when: Optional[Callable[[Exception], Coroutine[Any, Any, Any]]] = None,
    replace: Optional[Callable[[Exception], Coroutine[Any, Any, U]]] = None,
    do: Optional[Callable[[Exception], Coroutine[Any, Any, Any]]] = None,
    finally_raise: bool = False,
) -> Iterator[Union[T, U]]:
    return catch(
        iterator,
        errors,
        when=syncify(loop, when) if when else None,
        replace=syncify(loop, replace) if replace else None,
        do=syncify(loop, do) if do else None,
        finally_raise=finally_raise,
    )


def flatten(
    loop_getter: Callable[[], asyncio.AbstractEventLoop],
    iterator: Iterator[Union[Iterable[T], AsyncIterable[T]]],
    *,
    concurrency: int = 1,
) -> Iterator[T]:
    if concurrency == 1:
        return FlattenIterator(loop_getter, iterator)
    return ConcurrentFlattenIterator(
        loop_getter,
        iterator,
        concurrency=concurrency,
    )


def group(
    iterator: Iterator[T],
    up_to: Optional[int] = None,
    *,
    over: Optional[datetime.timedelta] = None,
    by: Optional[Callable[[T], Any]] = None,
) -> Iterator[List[T]]:
    if by is None:
        return GroupIterator(iterator, up_to, over)
    return map(itemgetter(1), GroupbyIterator(iterator, by, up_to, over))


def agroup(
    loop: asyncio.AbstractEventLoop,
    iterator: Iterator[T],
    up_to: Optional[int] = None,
    *,
    over: Optional[datetime.timedelta] = None,
    by: Optional[Callable[[T], Coroutine[Any, Any, Any]]] = None,
) -> Iterator[List[T]]:
    return group(
        iterator,
        up_to,
        over=over,
        by=syncify(loop, by) if by else None,
    )


def groupby(
    iterator: Iterator[T],
    key: Callable[[T], U],
    *,
    up_to: Optional[int] = None,
    over: Optional[datetime.timedelta] = None,
) -> Iterator[Tuple[U, List[T]]]:
    return GroupbyIterator(iterator, key, up_to, over)


def agroupby(
    loop: asyncio.AbstractEventLoop,
    iterator: Iterator[T],
    key: Callable[[T], Coroutine[Any, Any, U]],
    *,
    up_to: Optional[int] = None,
    over: Optional[datetime.timedelta] = None,
) -> Iterator[Tuple[U, List[T]]]:
    return groupby(
        iterator,
        syncify(loop, key),
        up_to=up_to,
        over=over,
    )


def map(
    into: Callable[[T], U],
    iterator: Iterator[T],
    *,
    concurrency: Union[int, Executor] = 1,
    ordered: bool = True,
) -> Iterator[U]:
    if concurrency == 1:
        return builtins.map(into, iterator)
    return ConcurrentMapIterator(
        iterator,
        into,
        concurrency=concurrency,
        ordered=ordered,
    )


def amap(
    loop: asyncio.AbstractEventLoop,
    into: Callable[[T], Coroutine[Any, Any, U]],
    iterator: Iterator[T],
    *,
    concurrency: int = 1,
    ordered: bool = True,
) -> Iterator[U]:
    if concurrency == 1:
        return map(syncify(loop, into), iterator)
    return ConcurrentAMapIterator(
        loop,
        iterator,
        into,
        concurrency=cast(int, concurrency),
        ordered=ordered,
    )


def observe(
    iterator: Iterator[T], label: str, every: Optional[Union[int, datetime.timedelta]]
) -> Iterator[T]:
    if every is None:
        return PowerObserveIterator(iterator, label)
    elif isinstance(every, int):
        return EveryIntObserveIterator(iterator, label, every)
    return EveryIntervalObserveIterator(iterator, label, every)


def skip(
    iterator: Iterator[T],
    until: Union[int, Callable[[T], Any]],
) -> Iterator[T]:
    if isinstance(until, int):
        return CountSkipIterator(iterator, until)
    return PredicateSkipIterator(iterator, until)


def askip(
    loop: asyncio.AbstractEventLoop,
    iterator: Iterator[T],
    until: Callable[[T], Coroutine[Any, Any, Any]],
) -> Iterator[T]:
    return PredicateSkipIterator(iterator, syncify(loop, until))


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
    when: Union[int, Callable[[T], Any]],
) -> Iterator[T]:
    if isinstance(when, int):
        return CountTruncateIterator(iterator, when)
    return PredicateTruncateIterator(iterator, when)


def atruncate(
    loop: asyncio.AbstractEventLoop,
    iterator: Iterator[T],
    when: Callable[[T], Coroutine[Any, Any, Any]],
) -> Iterator[T]:
    return PredicateTruncateIterator(iterator, syncify(loop, when))
