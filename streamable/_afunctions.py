from concurrent.futures import Executor
import datetime
from contextlib import suppress
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
    ACatchAsyncIterator,
    ADistinctAsyncIterator,
    AFilterAsyncIterator,
    AGroupbyAsyncIterator,
    AMapAsyncIterator,
    ConcurrentAMapAsyncIterator,
    ConcurrentFlattenAsyncIterator,
    ConcurrentMapAsyncIterator,
    ConsecutiveADistinctAsyncIterator,
    CountSkipAsyncIterator,
    CountTruncateAsyncIterator,
    FlattenAsyncIterator,
    GroupAsyncIterator,
    ObserveAsyncIterator,
    PredicateASkipAsyncIterator,
    PredicateATruncateAsyncIterator,
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
    when: Optional[Callable[[Exception], Any]] = None,
    replace: Optional[Callable[[Exception], U]] = None,
    finally_raise: bool = False,
) -> AsyncIterator[Union[T, U]]:
    return acatch(
        aiterator,
        errors,
        when=asyncify(when) if when else None,
        replace=asyncify(replace) if replace else None,
        finally_raise=finally_raise,
    )


def acatch(
    aiterator: AsyncIterator[T],
    errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
    *,
    when: Optional[Callable[[Exception], Coroutine[Any, Any, Any]]] = None,
    replace: Optional[Callable[[Exception], Coroutine[Any, Any, U]]] = None,
    finally_raise: bool = False,
) -> AsyncIterator[Union[T, U]]:
    return ACatchAsyncIterator(
        aiterator,
        errors,
        when=when,
        replace=replace,
        finally_raise=finally_raise,
    )


def distinct(
    aiterator: AsyncIterator[T],
    by: Optional[Callable[[T], Any]] = None,
    *,
    consecutive: bool = False,
) -> AsyncIterator[T]:
    return adistinct(
        aiterator,
        asyncify(by) if by else None,
        consecutive=consecutive,
    )


def adistinct(
    aiterator: AsyncIterator[T],
    by: Optional[Callable[[T], Coroutine[Any, Any, Any]]] = None,
    *,
    consecutive: bool = False,
) -> AsyncIterator[T]:
    if consecutive:
        return ConsecutiveADistinctAsyncIterator(aiterator, by)
    return ADistinctAsyncIterator(aiterator, by)


def filter(
    aiterator: AsyncIterator[T],
    where: Callable[[T], Any],
) -> AsyncIterator[T]:
    return afilter(aiterator, asyncify(where))


def afilter(
    aiterator: AsyncIterator[T],
    where: Callable[[T], Any],
) -> AsyncIterator[T]:
    return AFilterAsyncIterator(aiterator, where)


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
    by: Optional[Callable[[T], Any]] = None,
) -> AsyncIterator[List[T]]:
    return agroup(
        aiterator,
        up_to,
        over=over,
        by=asyncify(by) if by else None,
    )


def agroup(
    aiterator: AsyncIterator[T],
    up_to: Optional[int] = None,
    *,
    over: Optional[datetime.timedelta] = None,
    by: Optional[Callable[[T], Coroutine[Any, Any, Any]]] = None,
) -> AsyncIterator[List[T]]:
    if by is None:
        return GroupAsyncIterator(aiterator, up_to, over)
    return map(itemgetter(1), AGroupbyAsyncIterator(aiterator, by, up_to, over))


def groupby(
    aiterator: AsyncIterator[T],
    key: Callable[[T], U],
    *,
    up_to: Optional[int] = None,
    over: Optional[datetime.timedelta] = None,
) -> AsyncIterator[Tuple[U, List[T]]]:
    return agroupby(aiterator, asyncify(key), up_to=up_to, over=over)


def agroupby(
    aiterator: AsyncIterator[T],
    key: Callable[[T], Coroutine[Any, Any, U]],
    *,
    up_to: Optional[int] = None,
    over: Optional[datetime.timedelta] = None,
) -> AsyncIterator[Tuple[U, List[T]]]:
    return AGroupbyAsyncIterator(aiterator, key, up_to, over)


def map(
    to: Callable[[T], U],
    aiterator: AsyncIterator[T],
    *,
    concurrency: Union[int, Executor] = 1,
    ordered: bool = True,
) -> AsyncIterator[U]:
    if concurrency == 1:
        return amap(asyncify(to), aiterator)
    return ConcurrentMapAsyncIterator(
        aiterator,
        to,
        concurrency=concurrency,
        ordered=ordered,
    )


def amap(
    to: Callable[[T], Coroutine[Any, Any, U]],
    aiterator: AsyncIterator[T],
    *,
    concurrency: int = 1,
    ordered: bool = True,
) -> AsyncIterator[U]:
    if concurrency == 1:
        return AMapAsyncIterator(aiterator, to)
    return ConcurrentAMapAsyncIterator(
        aiterator,
        to,
        concurrency=cast(int, concurrency),
        ordered=ordered,
    )


def observe(aiterator: AsyncIterator[T], what: str) -> AsyncIterator[T]:
    return ObserveAsyncIterator(aiterator, what)


def skip(
    aiterator: AsyncIterator[T],
    until: Union[int, Callable[[T], Any]],
) -> AsyncIterator[T]:
    if isinstance(until, int):
        return CountSkipAsyncIterator(aiterator, until)
    return PredicateASkipAsyncIterator(aiterator, asyncify(until))


def askip(
    aiterator: AsyncIterator[T],
    until: Callable[[T], Coroutine[Any, Any, Any]],
) -> AsyncIterator[T]:
    return PredicateASkipAsyncIterator(aiterator, until)


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
    when: Union[int, Callable[[T], Any]],
) -> AsyncIterator[T]:
    if isinstance(when, int):
        return CountTruncateAsyncIterator(aiterator, when)
    return PredicateATruncateAsyncIterator(aiterator, asyncify(when))


def atruncate(
    aiterator: AsyncIterator[T],
    when: Callable[[T], Coroutine[Any, Any, Any]],
) -> AsyncIterator[T]:
    return PredicateATruncateAsyncIterator(aiterator, when)
