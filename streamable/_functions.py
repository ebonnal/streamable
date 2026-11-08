import datetime
from concurrent.futures import Executor
from operator import itemgetter
from typing import (
    AsyncIterable,
    Callable,
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

from streamable import _iterators
from streamable._tools._iter import ClosableIterator
from streamable._tools._observation import Observation

T = TypeVar("T")
U = TypeVar("U")
Exc = TypeVar("Exc", bound=Exception)


def buffer(
    upstream: ClosableIterator[T],
    up_to: Optional[int] = None,
) -> ClosableIterator[T]:
    return _iterators.BufferIterator(upstream, up_to)


def catch(
    upstream: ClosableIterator[T],
    errors: Union[Type[Exc], Tuple[Type[Exc], ...]],
    *,
    where: Optional[Callable[[Exc], object]] = None,
    replace: Optional[Callable[[Exc], U]] = None,
    do: Optional[Callable[[Exc], object]] = None,
    stop: bool = False,
) -> ClosableIterator[Union[T, U]]:
    return _iterators.CatchIterator(
        upstream,
        errors,
        where=where,
        replace=replace,
        do=do,
        stop=stop,
    )


def filter(
    where: Callable[[T], object],
    upstream: ClosableIterator[T],
) -> ClosableIterator[T]:
    # TODO relace
    return _iterators.FilterIterator(where, upstream)


def flatten(
    upstream: ClosableIterator[Union[Iterable[T], AsyncIterable[T]]],
    *,
    concurrency: Union[int, Executor] = 1,
) -> ClosableIterator[T]:
    if concurrency == 1:
        return _iterators.FlattenIterator(upstream)
    return _iterators.ConcurrentFlattenIterator(
        upstream,
        concurrency=concurrency,
    )


def group(
    upstream: ClosableIterator[T],
    up_to: Optional[int] = None,
    *,
    within: Optional[datetime.timedelta] = None,
    by: Union[None, Callable[[T], U]] = None,
) -> Union[Iterator[List[T]], Iterator[Tuple[U, List[T]]]]:
    if within is None:
        if by is None:
            return _iterators.GroupIterator(upstream, up_to=up_to)
        return _iterators.FlattenIterator(
            _iterators.GroupByIterator(upstream, by=by, up_to=up_to)
        )
    if by is None:
        return _iterators.MapIterator(
            itemgetter(1),
            _iterators.GroupByWithinIterator(
                upstream, by=lambda _: None, up_to=up_to, within=within
            ),
        )
    return _iterators.GroupByWithinIterator(upstream, by=by, up_to=up_to, within=within)


def map(
    into: Callable[[T], U],
    upstream: ClosableIterator[T],
    *,
    concurrency: Union[int, Executor] = 1,
    as_completed: bool = False,
) -> ClosableIterator[U]:
    if concurrency == 1:
        return _iterators.MapIterator(into, upstream)
    else:
        return _iterators.ConcurrentMapIterator(
            upstream,
            cast(Callable[[T], U], into),
            concurrency=concurrency,
            as_completed=as_completed,
        )


def observe(
    upstream: ClosableIterator[T],
    subject: str,
    every: Union[None, int, datetime.timedelta],
    do: Callable[[Observation], object],
) -> ClosableIterator[T]:
    if every is None:
        return _iterators.PowerObserveIterator(upstream, subject, do)
    elif isinstance(every, int):
        return _iterators.EveryIntObserveIterator(upstream, subject, every, do)
    return _iterators.EveryIntervalObserveIterator(upstream, subject, every, do)


def skip(
    upstream: ClosableIterator[T],
    until: Union[int, Callable[[T], object]],
) -> ClosableIterator[T]:
    if isinstance(until, int):
        return _iterators.CountSkipIterator(upstream, until)
    return _iterators.PredicateSkipIterator(upstream, until)


def take(
    upstream: ClosableIterator[T],
    until: Union[int, Callable[[T], object]],
) -> ClosableIterator[T]:
    if isinstance(until, int):
        return _iterators.CountTakeIterator(upstream, until)
    return _iterators.PredicateTakeIterator(upstream, until)


def throttle(
    upstream: ClosableIterator[T],
    count: int,
    *,
    per: datetime.timedelta,
) -> ClosableIterator[T]:
    return _iterators.ThrottleIterator(upstream, count, per)
