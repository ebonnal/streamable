from concurrent.futures import Executor
import datetime
from contextlib import suppress
from inspect import iscoroutinefunction
from operator import itemgetter
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Coroutine,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from streamable._aiterators import (
    CatchAsyncIterator,
    FilterAsyncIterator,
    GroupbyAsyncIterator,
    MapAsyncIterator,
    ConcurrentAMapAsyncIterator,
    ConcurrentFlattenAsyncIterator,
    ConcurrentMapAsyncIterator,
    CountSkipAsyncIterator,
    CountTruncateAsyncIterator,
    EveryIntObserveAsyncIterator,
    EveryIntervalObserveAsyncIterator,
    FlattenAsyncIterator,
    GroupAsyncIterator,
    PowerObserveAsyncIterator,
    PredicateSkipAsyncIterator,
    PredicateTruncateAsyncIterator,
    YieldsPerPeriodThrottleAsyncIterator,
)
from streamable._utils._func import asyncify

with suppress(ImportError):
    pass

T = TypeVar("T")
U = TypeVar("U")


def catch(
    aiterator: AsyncIterator[T],
    errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
    *,
    when: Optional[
        Union[
            Callable[[Exception], Any], Callable[[Exception], Coroutine[Any, Any, Any]]
        ]
    ] = None,
    replace: Optional[
        Union[Callable[[Exception], U], Callable[[Exception], Coroutine[Any, Any, U]]]
    ] = None,
    do: Optional[
        Union[
            Callable[[Exception], Any], Callable[[Exception], Coroutine[Any, Any, Any]]
        ]
    ] = None,
    finally_raise: bool = False,
) -> AsyncIterator[Union[T, U]]:
    return CatchAsyncIterator(
        aiterator,
        errors,
        when=when if not when or iscoroutinefunction(when) else asyncify(when),
        replace=replace
        if not replace or iscoroutinefunction(replace)
        else asyncify(replace),
        do=do if not do or iscoroutinefunction(do) else asyncify(do),
        finally_raise=finally_raise,
    )


def filter(
    where: Union[Callable[[T], Any], Callable[[T], Coroutine[Any, Any, Any]]],
    aiterator: AsyncIterator[T],
) -> AsyncIterator[T]:
    return FilterAsyncIterator(
        aiterator, where if not where or iscoroutinefunction(where) else asyncify(where)
    )


def flatten(
    aiterator: AsyncIterator[Union[Iterable[T], AsyncIterable[T]]],
    *,
    concurrency: int = 1,
) -> AsyncIterator[T]:
    if concurrency == 1:
        return FlattenAsyncIterator(aiterator)
    return ConcurrentFlattenAsyncIterator(
        aiterator,
        concurrency=concurrency,
    )


def group(
    aiterator: AsyncIterator[T],
    up_to: Optional[int] = None,
    *,
    over: Optional[datetime.timedelta] = None,
    by: Optional[
        Union[Callable[[T], Any], Callable[[T], Coroutine[Any, Any, Any]]]
    ] = None,
) -> AsyncIterator[List[T]]:
    if by is None:
        return GroupAsyncIterator(aiterator, up_to, over)
    return map(
        itemgetter(1),
        GroupbyAsyncIterator(
            aiterator,
            by=by if not by or iscoroutinefunction(by) else asyncify(by),
            up_to=up_to,
            over=over,
        ),
    )


def groupby(
    aiterator: AsyncIterator[T],
    by: Union[Callable[[T], U], Callable[[T], Coroutine[Any, Any, U]]],
    *,
    up_to: Optional[int] = None,
    over: Optional[datetime.timedelta] = None,
) -> AsyncIterator[Tuple[U, List[T]]]:
    return GroupbyAsyncIterator(
        aiterator,
        cast(
            Callable[[T], Coroutine[Any, Any, Any]],
            by if not by or iscoroutinefunction(by) else asyncify(by),
        ),
        up_to,
        over,
    )


def map(
    into: Union[Callable[[T], U], Callable[[T], Coroutine[Any, Any, U]]],
    aiterator: AsyncIterator[T],
    *,
    concurrency: Union[int, Executor] = 1,
    ordered: bool = True,
) -> AsyncIterator[U]:
    if concurrency == 1:
        return MapAsyncIterator(
            aiterator,
            cast(
                Callable[[T], Coroutine[Any, Any, U]],
                into if not into or iscoroutinefunction(into) else asyncify(into),
            ),
        )
    if iscoroutinefunction(into):
        return ConcurrentAMapAsyncIterator(
            aiterator,
            cast(Callable[[T], Coroutine[Any, Any, U]], into),
            concurrency=cast(int, concurrency),
            ordered=ordered,
        )
    else:
        return ConcurrentMapAsyncIterator(
            aiterator,
            cast(Callable[[T], U], into),
            concurrency=concurrency,
            ordered=ordered,
        )


def observe(
    aiterator: AsyncIterator[T],
    label: str,
    every: Optional[Union[int, datetime.timedelta]],
) -> AsyncIterator[T]:
    if every is None:
        return PowerObserveAsyncIterator(aiterator, label)
    elif isinstance(every, int):
        return EveryIntObserveAsyncIterator(aiterator, label, every)
    return EveryIntervalObserveAsyncIterator(aiterator, label, every)


def skip(
    aiterator: AsyncIterator[T],
    until: Union[int, Callable[[T], Any], Callable[[T], Coroutine[Any, Any, Any]]],
) -> AsyncIterator[T]:
    if isinstance(until, int):
        return CountSkipAsyncIterator(aiterator, until)
    return PredicateSkipAsyncIterator(
        aiterator, until if not until or iscoroutinefunction(until) else asyncify(until)
    )


def throttle(
    aiterator: AsyncIterator[T],
    count: Optional[int],
    *,
    per: Optional[datetime.timedelta] = None,
) -> AsyncIterator[T]:
    if count and per:
        aiterator = YieldsPerPeriodThrottleAsyncIterator(aiterator, count, per)
    return aiterator


def truncate(
    aiterator: AsyncIterator[T],
    when: Union[int, Callable[[T], Any], Callable[[T], Coroutine[Any, Any, Any]]],
) -> AsyncIterator[T]:
    if isinstance(when, int):
        return CountTruncateAsyncIterator(aiterator, when)
    return PredicateTruncateAsyncIterator(
        aiterator, when if not when or iscoroutinefunction(when) else asyncify(when)
    )
