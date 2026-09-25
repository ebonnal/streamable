import builtins
from concurrent.futures import Executor
import datetime
from operator import itemgetter
from typing import (
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

from streamable._tools._observation import Observation

from streamable import _iterators

T = TypeVar("T")
U = TypeVar("U")
Exc = TypeVar("Exc", bound=Exception)


def buffer(
    upstream: Iterator[T],
    up_to: Optional[int] = None,
) -> Iterator[T]:
    return _iterators.BufferIterator(upstream, up_to)


def catch(
    upstream: Iterator[T],
    errors: Union[Type[Exc], Tuple[Type[Exc], ...]],
    *,
    where: Optional[Union[Callable[[Exc], object]]] = None,
    replace: Optional[Union[Callable[[Exc], U]]] = None,
    do: Optional[Union[Callable[[Exc], object]]] = None,
    stop: bool = False,
) -> Iterator[Union[T, U]]:
    return _iterators.CatchIterator(
        upstream,
        errors,
        where=where,
        replace=replace,
        do=do,
        stop=stop,
    )


def filter(
    where: Union[Callable[[T], object]],
    upstream: Iterator[T],
) -> Iterator[T]:
    return builtins.filter(where, upstream)


def flatten(
    upstream: Iterator[Iterable[T]],
    *,
    concurrency: int = 1,
) -> Iterator[T]:
    if concurrency == 1:
        return _iterators.FlattenIterator(upstream)
    return _iterators.ConcurrentFlattenIterator(
        upstream,
        concurrency=concurrency,
    )


def group(
    upstream: Iterator[T],
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
        return builtins.map(
            itemgetter(1),
            _iterators.GroupByWithinIterator(
                upstream, by=lambda _: None, up_to=up_to, within=within
            ),
        )
    return _iterators.GroupByWithinIterator(upstream, by=by, up_to=up_to, within=within)


def map(
    into: Union[Callable[[T], U]],
    upstream: Iterator[T],
    *,
    concurrency: Union[int, Executor] = 1,
    as_completed: bool = False,
) -> Iterator[U]:
    if concurrency == 1:
        return builtins.map(into, upstream)
    else:
        return _iterators.ConcurrentMapIterator(
            upstream,
            cast(Callable[[T], U], into),
            concurrency=concurrency,
            as_completed=as_completed,
        )


def observe(
    upstream: Iterator[T],
    subject: str,
    every: Union[None, int, datetime.timedelta],
    do: Union[Callable[[Observation], object],],
) -> Iterator[T]:
    if every is None:
        return _iterators.PowerObserveIterator(upstream, subject, do)
    elif isinstance(every, int):
        return _iterators.EveryIntObserveIterator(upstream, subject, every, do)
    return _iterators.EveryIntervalObserveIterator(upstream, subject, every, do)


def skip(
    upstream: Iterator[T],
    until: Union[int, Callable[[T], object]],
) -> Iterator[T]:
    if isinstance(until, int):
        return _iterators.CountSkipIterator(upstream, until)
    return _iterators.PredicateSkipIterator(upstream, until)


def take(
    upstream: Iterator[T],
    until: Union[int, Callable[[T], object]],
) -> Iterator[T]:
    if isinstance(until, int):
        return _iterators.CountTakeIterator(upstream, until)
    return _iterators.PredicateTakeIterator(upstream, until)


def throttle(
    upstream: Iterator[T],
    count: int,
    *,
    per: datetime.timedelta,
) -> Iterator[T]:
    return _iterators.ThrottleIterator(upstream, count, per)
