import builtins
import datetime
from contextlib import suppress
from operator import itemgetter
from typing import (
    Any,
    Callable,
    Coroutine,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    cast,
)

from streamable.iterators import (
    AsyncConcurrentMapIterator,
    CatchIterator,
    ConcurrentFlattenIterator,
    ConsecutiveDistinctIterator,
    CountTruncateIterator,
    DistinctIterator,
    FlattenIterator,
    GroupbyIterator,
    GroupIterator,
    IntervalThrottleIterator,
    ObserveIterator,
    OSConcurrentMapIterator,
    PredicateTruncateIterator,
    SkipIterator,
    YieldsPerPeriodThrottleIterator,
)
from streamable.util.constants import NO_REPLACEMENT
from streamable.util.functiontools import wrap_error
from streamable.util.validationtools import (
    validate_concurrency,
    validate_count,
    validate_group_interval,
    validate_group_size,
    validate_iterator,
    validate_throttle_interval,
    validate_throttle_per_period,
    validate_truncate_args,
)

with suppress(ImportError):
    from typing import Literal

T = TypeVar("T")
U = TypeVar("U")


def catch(
    iterator: Iterator[T],
    kind: Type[Exception] = Exception,
    when: Callable[[Exception], Any] = bool,
    replacement: T = NO_REPLACEMENT,  # type: ignore
    finally_raise: bool = False,
) -> Iterator[T]:
    validate_iterator(iterator)
    return CatchIterator(
        iterator,
        kind,
        when,
        replacement,
        finally_raise=finally_raise,
    )


def distinct(
    iterator: Iterator[T],
    by: Optional[Callable[[T], Any]] = None,
    consecutive_only: bool = False,
) -> Iterator[T]:
    validate_iterator(iterator)
    if consecutive_only:
        return ConsecutiveDistinctIterator(iterator, by)
    return DistinctIterator(iterator, by)


def flatten(iterator: Iterator[Iterable[T]], concurrency: int = 1) -> Iterator[T]:
    validate_iterator(iterator)
    validate_concurrency(concurrency)
    if concurrency == 1:
        return FlattenIterator(iterator)
    else:
        return ConcurrentFlattenIterator(
            iterator,
            concurrency=concurrency,
            buffersize=concurrency,
        )


def group(
    iterator: Iterator[T],
    size: Optional[int] = None,
    interval: Optional[datetime.timedelta] = None,
    by: Optional[Callable[[T], Any]] = None,
) -> Iterator[List[T]]:
    validate_iterator(iterator)
    validate_group_size(size)
    validate_group_interval(interval)
    if by is None:
        return GroupIterator(iterator, size, interval)
    return map(itemgetter(1), GroupbyIterator(iterator, by, size, interval))


def groupby(
    iterator: Iterator[T],
    by: Callable[[T], U],
    size: Optional[int] = None,
    interval: Optional[datetime.timedelta] = None,
) -> Iterator[Tuple[U, List[T]]]:
    validate_iterator(iterator)
    validate_group_size(size)
    validate_group_interval(interval)
    return GroupbyIterator(iterator, by, size, interval)


def map(
    transformation: Callable[[T], U],
    iterator: Iterator[T],
    concurrency: int = 1,
    ordered: bool = True,
    via: "Literal['thread', 'process']" = "thread",
) -> Iterator[U]:
    validate_iterator(iterator)
    validate_concurrency(concurrency)
    if concurrency == 1:
        return builtins.map(wrap_error(transformation, StopIteration), iterator)
    else:
        return OSConcurrentMapIterator(
            iterator,
            transformation,
            concurrency=concurrency,
            buffersize=concurrency,
            ordered=ordered,
            via=via,
        )


def amap(
    transformation: Callable[[T], Coroutine[Any, Any, U]],
    iterator: Iterator[T],
    concurrency: int = 1,
    ordered: bool = True,
) -> Iterator[U]:
    validate_iterator(iterator)
    validate_concurrency(concurrency)
    return AsyncConcurrentMapIterator(
        iterator,
        transformation,
        buffersize=concurrency,
        ordered=ordered,
    )


def observe(iterator: Iterator[T], what: str) -> Iterator[T]:
    validate_iterator(iterator)
    return ObserveIterator(iterator, what)


def skip(
    iterator: Iterator[T],
    count: int,
) -> Iterator[T]:
    validate_iterator(iterator)
    validate_count(count)
    if count > 0:
        iterator = SkipIterator(iterator, count)
    return iterator


def throttle(
    iterator: Iterator[T],
    per_second: int = cast(int, float("inf")),
    per_minute: int = cast(int, float("inf")),
    per_hour: int = cast(int, float("inf")),
    interval: datetime.timedelta = datetime.timedelta(0),
) -> Iterator[T]:
    validate_iterator(iterator)
    validate_throttle_per_period("per_second", per_second)
    validate_throttle_per_period("per_minute", per_minute)
    validate_throttle_per_period("per_hour", per_hour)
    validate_throttle_interval(interval)

    for per_period, period in (
        (per_second, datetime.timedelta(seconds=1)),
        (per_minute, datetime.timedelta(minutes=1)),
        (per_hour, datetime.timedelta(hours=1)),
    ):
        if per_period < float("inf"):
            iterator = YieldsPerPeriodThrottleIterator(iterator, per_period, period)

    if interval > datetime.timedelta(0):
        iterator = IntervalThrottleIterator(iterator, interval)
    return iterator


def truncate(
    iterator: Iterator[T],
    count: Optional[int] = None,
    when: Optional[Callable[[T], Any]] = None,
) -> Iterator[T]:
    validate_iterator(iterator)
    validate_truncate_args(count, when)
    if count is not None:
        iterator = CountTruncateIterator(iterator, count)
    if when is not None:
        iterator = PredicateTruncateIterator(iterator, when)
    return iterator
