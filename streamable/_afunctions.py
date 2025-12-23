from concurrent.futures import Executor
import datetime
from contextlib import suppress
from inspect import iscoroutinefunction
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from streamable import _aiterators
from streamable._tools._async import AsyncCallable
from streamable._tools._func import asyncify

with suppress(ImportError):
    pass

T = TypeVar("T")
U = TypeVar("U")


def catch(
    aiterator: AsyncIterator[T],
    errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
    *,
    where: Optional[
        Union[Callable[[Exception], Any], AsyncCallable[Exception, Any]]
    ] = None,
    replace: Optional[
        Union[Callable[[Exception], U], AsyncCallable[Exception, U]]
    ] = None,
    do: Optional[
        Union[Callable[[Exception], Any], AsyncCallable[Exception, Any]]
    ] = None,
    stop: bool = False,
) -> AsyncIterator[Union[T, U]]:
    return _aiterators.CatchAsyncIterator(
        aiterator,
        errors,
        where=asyncify(where),
        replace=asyncify(replace),
        do=asyncify(do),
        stop=stop,
    )


def filter(
    where: Union[Callable[[T], Any], AsyncCallable[T, Any]],
    aiterator: AsyncIterator[T],
) -> AsyncIterator[T]:
    return _aiterators.FilterAsyncIterator(aiterator, asyncify(where))


def flatten(
    aiterator: AsyncIterator[Union[Iterable[T], AsyncIterable[T]]],
    *,
    concurrency: int = 1,
) -> AsyncIterator[T]:
    if concurrency == 1:
        return _aiterators.FlattenAsyncIterator(aiterator)
    return _aiterators.ConcurrentFlattenAsyncIterator(
        aiterator,
        concurrency=concurrency,
    )


def group(
    aiterator: AsyncIterator[T],
    up_to: Optional[int] = None,
    *,
    every: Optional[datetime.timedelta] = None,
    by: Union[None, Callable[[T], U], AsyncCallable[T, U]] = None,
) -> Union[AsyncIterator[List[T]], AsyncIterator[Tuple[U, List[T]]]]:
    if by is None:
        return _aiterators.GroupAsyncIterator(aiterator, up_to, every)
    return _aiterators.GroupbyAsyncIterator(
        aiterator,
        by=asyncify(by),
        up_to=up_to,
        every=every,
    )


def map(
    into: Union[Callable[[T], U], AsyncCallable[T, U]],
    aiterator: AsyncIterator[T],
    *,
    concurrency: Union[int, Executor] = 1,
    ordered: bool = True,
) -> AsyncIterator[U]:
    if concurrency == 1:
        return _aiterators.MapAsyncIterator(aiterator, asyncify(into))
    if iscoroutinefunction(into):
        return _aiterators.ConcurrentAMapAsyncIterator(
            aiterator,
            cast(AsyncCallable[T, U], into),
            concurrency=cast(int, concurrency),
            ordered=ordered,
        )
    else:
        return _aiterators.ConcurrentMapAsyncIterator(
            aiterator,
            cast(Callable[[T], U], into),
            concurrency=concurrency,
            ordered=ordered,
        )


def observe(
    aiterator: AsyncIterator[T],
    subject: str,
    every: Union[None, int, datetime.timedelta],
    how: Union[None, Callable[[str], Any], AsyncCallable[str, Any]] = None,
) -> AsyncIterator[T]:
    if every is None:
        return _aiterators.PowerObserveAsyncIterator(aiterator, subject, asyncify(how))
    elif isinstance(every, int):
        return _aiterators.EveryIntObserveAsyncIterator(
            aiterator, subject, every, asyncify(how)
        )
    return _aiterators.EveryIntervalObserveAsyncIterator(
        aiterator, subject, every, asyncify(how)
    )


def skip(
    aiterator: AsyncIterator[T],
    until: Union[int, Callable[[T], Any], AsyncCallable[T, Any]],
) -> AsyncIterator[T]:
    if isinstance(until, int):
        return _aiterators.CountSkipAsyncIterator(aiterator, until)
    return _aiterators.PredicateSkipAsyncIterator(aiterator, asyncify(until))


def take(
    aiterator: AsyncIterator[T],
    until: Union[int, Callable[[T], Any], AsyncCallable[T, Any]],
) -> AsyncIterator[T]:
    if isinstance(until, int):
        return _aiterators.CountTakeAsyncIterator(aiterator, until)
    return _aiterators.PredicateTakeAsyncIterator(aiterator, asyncify(until))


def throttle(
    aiterator: AsyncIterator[T],
    count: int,
    *,
    per: datetime.timedelta,
) -> AsyncIterator[T]:
    return _aiterators.ThrottleAsyncIterator(aiterator, count, per)
