import asyncio
import builtins
from concurrent.futures import Executor
import datetime
from contextlib import suppress
from inspect import iscoroutinefunction
from typing import (
    TYPE_CHECKING,
    Any,
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

if TYPE_CHECKING:
    from streamable._stream import stream

from streamable import _iterators
from streamable._tools._async import AsyncCallable
from streamable._tools._func import syncify

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
        Union[Callable[[Exception], Any], AsyncCallable[Exception, Any]]
    ] = None,
    replace: Optional[
        Union[Callable[[Exception], U], AsyncCallable[Exception, U]]
    ] = None,
    do: Optional[
        Union[Callable[[Exception], Any], AsyncCallable[Exception, Any]]
    ] = None,
    stop: bool = False,
) -> Iterator[Union[T, U]]:
    return _iterators.CatchIterator(
        iterator,
        errors,
        where=syncify(loop_getter, where),
        replace=syncify(loop_getter, replace),
        do=syncify(loop_getter, do),
        stop=stop,
    )


def filter(
    loop_getter: Callable[[], asyncio.AbstractEventLoop],
    where: Union[Callable[[T], Any], AsyncCallable[T, Any]],
    iterator: Iterator[T],
) -> Iterator[T]:
    return builtins.filter(syncify(loop_getter, where), iterator)


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
    by: Union[None, Callable[[T], U], AsyncCallable[T, U]] = None,
) -> Union[Iterator[List[T]], Iterator[Tuple[U, List[T]]]]:
    if by is None:
        return _iterators.GroupIterator(iterator, up_to, every)
    return _iterators.GroupbyIterator(
        iterator,
        by=syncify(loop_getter, by),
        up_to=up_to,
        every=every,
    )


def map(
    loop_getter: Callable[[], asyncio.AbstractEventLoop],
    into: Union[Callable[[T], U], AsyncCallable[T, U]],
    iterator: Iterator[T],
    *,
    concurrency: Union[int, Executor] = 1,
    ordered: bool = True,
) -> Iterator[U]:
    if concurrency == 1:
        return builtins.map(syncify(loop_getter, into), iterator)
    if iscoroutinefunction(into):
        return _iterators.AsyncConcurrentMapIterator(
            loop_getter(),
            iterator,
            into,
            concurrency=cast(int, concurrency),
            ordered=ordered,
        )
    else:
        return _iterators.ExecutorConcurrentMapIterator(
            iterator,
            cast(Callable[[T], U], into),
            concurrency=concurrency,
            ordered=ordered,
        )


def observe(
    loop_getter: Callable[[], asyncio.AbstractEventLoop],
    iterator: Iterator[T],
    subject: str,
    every: Union[None, int, datetime.timedelta],
    do: Union[
        Callable[["stream.Observation"], Any],
        AsyncCallable["stream.Observation", Any],
    ],
) -> Iterator[T]:
    if every is None:
        return _iterators.PowerObserveIterator(
            iterator, subject, syncify(loop_getter, do)
        )
    elif isinstance(every, int):
        return _iterators.EveryIntObserveIterator(
            iterator, subject, every, syncify(loop_getter, do)
        )
    return _iterators.EveryIntervalObserveIterator(
        iterator, subject, every, syncify(loop_getter, do)
    )


def skip(
    loop_getter: Callable[[], asyncio.AbstractEventLoop],
    iterator: Iterator[T],
    until: Union[int, Callable[[T], Any], AsyncCallable[T, Any]],
) -> Iterator[T]:
    if isinstance(until, int):
        return _iterators.CountSkipIterator(iterator, until)
    return _iterators.PredicateSkipIterator(iterator, syncify(loop_getter, until))


def take(
    loop_getter: Callable[[], asyncio.AbstractEventLoop],
    iterator: Iterator[T],
    until: Union[int, Callable[[T], Any], AsyncCallable[T, Any]],
) -> Iterator[T]:
    if isinstance(until, int):
        return _iterators.CountTakeIterator(iterator, until)
    return _iterators.PredicateTakeIterator(iterator, syncify(loop_getter, until))


def throttle(
    iterator: Iterator[T],
    count: int,
    *,
    per: datetime.timedelta,
) -> Iterator[T]:
    return _iterators.ThrottleIterator(iterator, count, per)
