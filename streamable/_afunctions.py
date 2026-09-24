from concurrent.futures import Executor
import datetime
from inspect import iscoroutinefunction
from operator import itemgetter
from typing import (
    AsyncIterable,
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

from streamable._tools._iter import ClosableAsyncIterator
from streamable._tools._observation import Observation

from streamable import _aiterators
from streamable._tools._async import AsyncFunction
from streamable._tools._func import asyncify

T = TypeVar("T")
U = TypeVar("U")
Exc = TypeVar("Exc", bound=Exception)


def buffer(
    upstream: ClosableAsyncIterator[T],
    up_to: Optional[int] = None,
) -> ClosableAsyncIterator[T]:
    return _aiterators.BufferAsyncIterator(upstream, up_to)


def catch(
    upstream: ClosableAsyncIterator[T],
    errors: Union[Type[Exc], Tuple[Type[Exc], ...]],
    *,
    where: Optional[Union[Callable[[Exc], object], AsyncFunction[Exc, object]]] = None,
    replace: Optional[Union[Callable[[Exc], U], AsyncFunction[Exc, U]]] = None,
    do: Optional[Union[Callable[[Exc], object], AsyncFunction[Exc, object]]] = None,
    stop: bool = False,
) -> ClosableAsyncIterator[Union[T, U]]:
    return _aiterators.CatchAsyncIterator(
        upstream,
        errors,
        where=asyncify(where),
        replace=asyncify(replace),
        do=asyncify(do),
        stop=stop,
    )


def filter(
    where: Union[Callable[[T], object], AsyncFunction[T, object]],
    upstream: ClosableAsyncIterator[T],
) -> ClosableAsyncIterator[T]:
    return _aiterators.FilterAsyncIterator(upstream, asyncify(where))


def flatten(
    upstream: ClosableAsyncIterator[Union[Iterable[T], AsyncIterable[T]]],
    *,
    concurrency: int = 1,
) -> ClosableAsyncIterator[T]:
    if concurrency == 1:
        return _aiterators.FlattenAsyncIterator(upstream)
    return _aiterators.ConcurrentFlattenAsyncIterator(
        upstream,
        concurrency=concurrency,
    )


def group(
    upstream: ClosableAsyncIterator[T],
    up_to: Optional[int] = None,
    *,
    within: Optional[datetime.timedelta] = None,
    by: Union[None, Callable[[T], U], AsyncFunction[T, U]] = None,
) -> Union[ClosableAsyncIterator[List[T]], ClosableAsyncIterator[Tuple[U, List[T]]]]:
    if within is None:
        if by is None:
            return _aiterators.GroupAsyncIterator(upstream, up_to=up_to)
        return _aiterators.FlattenAsyncIterator(
            _aiterators.GroupByAsyncIterator(
                upstream, by=cast(AsyncFunction[T, U], asyncify(by)), up_to=up_to
            )
        )
    if by is None:
        return _aiterators.MapAsyncIterator(
            _aiterators.GroupByWithinAsyncIterator(
                upstream,
                by=asyncify(lambda _: None),
                up_to=up_to,
                within=within,
            ),
            asyncify(itemgetter(1)),
        )
    return _aiterators.GroupByWithinAsyncIterator(
        upstream, by=asyncify(by), up_to=up_to, within=within
    )


def map(
    into: Union[Callable[[T], U], AsyncFunction[T, U]],
    upstream: ClosableAsyncIterator[T],
    *,
    concurrency: Union[int, Executor] = 1,
    as_completed: bool = False,
) -> ClosableAsyncIterator[U]:
    if concurrency == 1:
        return _aiterators.MapAsyncIterator(upstream, asyncify(into))
    if iscoroutinefunction(into):
        return _aiterators.AsyncConcurrentMapAsyncIterator(
            upstream,
            cast(AsyncFunction[T, U], into),
            concurrency=cast(int, concurrency),
            as_completed=as_completed,
        )
    else:
        return _aiterators.ExecutorConcurrentMapAsyncIterator(
            upstream,
            cast(Callable[[T], U], into),
            concurrency=concurrency,
            as_completed=as_completed,
        )


def observe(
    upstream: ClosableAsyncIterator[T],
    subject: str,
    every: Union[None, int, datetime.timedelta],
    do: Union[
        Callable[[Observation], object],
        AsyncFunction[Observation, object],
    ],
) -> ClosableAsyncIterator[T]:
    if every is None:
        return _aiterators.PowerObserveAsyncIterator(upstream, subject, asyncify(do))
    elif isinstance(every, int):
        return _aiterators.EveryIntObserveAsyncIterator(
            upstream, subject, every, asyncify(do)
        )
    return _aiterators.EveryIntervalObserveAsyncIterator(
        upstream, subject, every, asyncify(do)
    )


def skip(
    upstream: ClosableAsyncIterator[T],
    until: Union[int, Callable[[T], object], AsyncFunction[T, object]],
) -> ClosableAsyncIterator[T]:
    if isinstance(until, int):
        return _aiterators.CountSkipAsyncIterator(upstream, until)
    return _aiterators.PredicateSkipAsyncIterator(upstream, asyncify(until))


def take(
    upstream: ClosableAsyncIterator[T],
    until: Union[int, Callable[[T], object], AsyncFunction[T, object]],
) -> ClosableAsyncIterator[T]:
    if isinstance(until, int):
        return _aiterators.CountTakeAsyncIterator(upstream, until)
    return _aiterators.PredicateTakeAsyncIterator(upstream, asyncify(until))


def throttle(
    upstream: ClosableAsyncIterator[T],
    count: int,
    *,
    per: datetime.timedelta,
) -> ClosableAsyncIterator[T]:
    return _aiterators.ThrottleAsyncIterator(upstream, count, per)
