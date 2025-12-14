import asyncio
import builtins
from concurrent.futures import Executor
import datetime
from contextlib import suppress
from inspect import iscoroutinefunction
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

from streamable import _iterators
from streamable._utils._func import syncify

with suppress(ImportError):
    pass

T = TypeVar("T")
U = TypeVar("U")


def catch(
    loop_getter: Callable[[], asyncio.AbstractEventLoop],
    iterator: Iterator[T],
    errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
    *,
    where: Optional[
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
    stop: bool = False,
) -> Iterator[Union[T, U]]:
    return _iterators.CatchIterator(
        iterator,
        errors,
        where=syncify(loop_getter(), where),
        replace=syncify(loop_getter(), replace),
        do=syncify(loop_getter(), do),
        stop=stop,
    )


def filter(
    loop_getter: Callable[[], asyncio.AbstractEventLoop],
    where: Union[Callable[[T], Any], Callable[[T], Coroutine[Any, Any, Any]]],
    iterator: Iterator[T],
) -> Iterator[T]:
    return builtins.filter(syncify(loop_getter(), where), iterator)


def flatten(
    loop_getter: Callable[[], asyncio.AbstractEventLoop],
    iterator: Iterator[Union[Iterable[T], AsyncIterable[T]]],
    *,
    concurrency: int = 1,
) -> Iterator[T]:
    if concurrency == 1:
        return _iterators.FlattenIterator(loop_getter, iterator)
    return _iterators.ConcurrentFlattenIterator(
        loop_getter,
        iterator,
        concurrency=concurrency,
    )


def group(
    loop_getter: Callable[[], asyncio.AbstractEventLoop],
    iterator: Iterator[T],
    up_to: Optional[int] = None,
    *,
    every: Optional[datetime.timedelta] = None,
    by: Optional[Union[Callable[[T], U], Callable[[T], Coroutine[Any, Any, U]]]] = None,
) -> Union[Iterator[List[T]], Iterator[Tuple[U, List[T]]]]:
    if by is None:
        return _iterators.GroupIterator(iterator, up_to, every)
    return _iterators.GroupbyIterator(
        iterator,
        by=syncify(loop_getter(), by),
        up_to=up_to,
        every=every,
    )


def map(
    loop_getter: Callable[[], asyncio.AbstractEventLoop],
    into: Union[Callable[[T], U], Callable[[T], Coroutine[Any, Any, U]]],
    iterator: Iterator[T],
    *,
    concurrency: Union[int, Executor] = 1,
    ordered: bool = True,
) -> Iterator[U]:
    if concurrency == 1:
        return builtins.map(syncify(loop_getter(), into), iterator)
    if iscoroutinefunction(into):
        return _iterators.ConcurrentAMapIterator(
            loop_getter(),
            iterator,
            into,
            concurrency=cast(int, concurrency),
            ordered=ordered,
        )
    else:
        return _iterators.ConcurrentMapIterator(
            iterator,
            cast(Callable[[T], U], into),
            concurrency=concurrency,
            ordered=ordered,
        )


def observe(
    iterator: Iterator[T],
    label: str,
    every: Optional[Union[int, datetime.timedelta]],
    format: Optional[str],
) -> Iterator[T]:
    if every is None:
        return _iterators.PowerObserveIterator(iterator, label, format)
    elif isinstance(every, int):
        return _iterators.EveryIntObserveIterator(iterator, label, format, every)
    return _iterators.EveryIntervalObserveIterator(iterator, label, format, every)


def skip(
    loop_getter: Callable[[], asyncio.AbstractEventLoop],
    iterator: Iterator[T],
    until: Union[int, Callable[[T], Any], Callable[[T], Coroutine[Any, Any, Any]]],
) -> Iterator[T]:
    if isinstance(until, int):
        return _iterators.CountSkipIterator(iterator, until)
    return _iterators.PredicateSkipIterator(iterator, syncify(loop_getter(), until))


def throttle(
    iterator: Iterator[T],
    count: Optional[int],
    *,
    per: Optional[datetime.timedelta] = None,
) -> Iterator[T]:
    if count and per:
        iterator = _iterators.ThrottleIterator(iterator, count, per)
    return iterator


def take(
    loop_getter: Callable[[], asyncio.AbstractEventLoop],
    iterator: Iterator[T],
    until: Union[int, Callable[[T], Any], Callable[[T], Coroutine[Any, Any, Any]]],
) -> Iterator[T]:
    if isinstance(until, int):
        return _iterators.CountTakeIterator(iterator, until)
    return _iterators.PredicateTakeIterator(iterator, syncify(loop_getter(), until))
