import builtins
from concurrent.futures import Executor
import datetime
from contextlib import suppress
from operator import itemgetter
from typing import (
    Any,
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

with suppress(ImportError):
    pass

T = TypeVar("T")
U = TypeVar("U")
Exc = TypeVar("Exc", bound=Exception)


def buffer(
    iterator: Iterator[T],
    up_to: int,
) -> Iterator[T]:
    return _iterators.BufferIterator(iterator, up_to)


def catch(
    iterator: Iterator[T],
    errors: Union[Type[Exc], Tuple[Type[Exc], ...]],
    *,
    where: Optional[Union[Callable[[Exc], Any]]] = None,
    replace: Optional[Union[Callable[[Exc], U]]] = None,
    do: Optional[Union[Callable[[Exc], Any]]] = None,
    stop: bool = False,
) -> Iterator[Union[T, U]]:
    return _iterators.CatchIterator(
        iterator,
        errors,
        where=where,
        replace=replace,
        do=do,
        stop=stop,
    )


def filter(
    where: Union[Callable[[T], Any]],
    iterator: Iterator[T],
) -> Iterator[T]:
    return builtins.filter(where, iterator)


def flatten(
    iterator: Iterator[Iterable[T]],
    *,
    concurrency: int = 1,
) -> Iterator[T]:
    if concurrency == 1:
        return _iterators.FlattenIterator(iterator)
    return _iterators.ConcurrentFlattenIterator(
        iterator,
        concurrency=concurrency,
    )


def group(
    iterator: Iterator[T],
    up_to: Optional[int] = None,
    *,
    within: Optional[datetime.timedelta] = None,
    by: Union[None, Callable[[T], U]] = None,
) -> Union[Iterator[List[T]], Iterator[Tuple[U, List[T]]]]:
    if within is None:
        if by is None:
            return _iterators.GroupIterator(iterator, up_to=up_to)
        return _iterators.FlattenIterator(
            _iterators.GroupByIterator(iterator, by=by, up_to=up_to)
        )
    if by is None:
        return builtins.map(
            itemgetter(1),
            _iterators.GroupByWithinIterator(
                iterator, by=lambda _: None, up_to=up_to, within=within
            ),
        )
    return _iterators.GroupByWithinIterator(iterator, by=by, up_to=up_to, within=within)


def map(
    into: Union[Callable[[T], U]],
    iterator: Iterator[T],
    *,
    concurrency: Union[int, Executor] = 1,
    as_completed: bool = False,
) -> Iterator[U]:
    if concurrency == 1:
        return builtins.map(into, iterator)
    else:
        return _iterators.ConcurrentMapIterator(
            iterator,
            cast(Callable[[T], U], into),
            concurrency=concurrency,
            as_completed=as_completed,
        )


def observe(
    iterator: Iterator[T],
    subject: str,
    every: Union[None, int, datetime.timedelta],
    do: Union[Callable[[Observation], Any],],
) -> Iterator[T]:
    if every is None:
        return _iterators.PowerObserveIterator(iterator, subject, do)
    elif isinstance(every, int):
        return _iterators.EveryIntObserveIterator(iterator, subject, every, do)
    return _iterators.EveryIntervalObserveIterator(iterator, subject, every, do)


def skip(
    iterator: Iterator[T],
    until: Union[int, Callable[[T], Any]],
) -> Iterator[T]:
    if isinstance(until, int):
        return _iterators.CountSkipIterator(iterator, until)
    return _iterators.PredicateSkipIterator(iterator, until)


def take(
    iterator: Iterator[T],
    until: Union[int, Callable[[T], Any]],
) -> Iterator[T]:
    if isinstance(until, int):
        return _iterators.CountTakeIterator(iterator, until)
    return _iterators.PredicateTakeIterator(iterator, until)


def throttle(
    iterator: Iterator[T],
    count: int,
    *,
    per: datetime.timedelta,
) -> Iterator[T]:
    return _iterators.ThrottleIterator(iterator, count, per)
