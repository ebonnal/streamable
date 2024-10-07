import builtins
import datetime
from typing import (
    Any,
    Callable,
    Coroutine,
    Iterable,
    Iterator,
    List,
    Optional,
    Type,
    TypeVar,
    cast,
)

from streamable.iters import (
    AsyncConcurrentMappingIterable,
    CatchingIterator,
    ConcurrentFlatteningIterable,
    FlatteningIterator,
    GroupingByIterator,
    GroupingIterator,
    ObservingIterator,
    OSConcurrentMappingIterable,
    RaisingIterator,
    ThrottlingIntervalIterator,
    ThrottlingPerPeriodIterator,
    TruncatingOnCountIterator,
    TruncatingOnPredicateIterator,
)
from streamable.util.constants import NO_REPLACEMENT
from streamable.util.exceptions import NoopStopIteration
from streamable.util.functiontools import reraise_as
from streamable.util.validationtools import (
    validate_concurrency,
    validate_group_interval,
    validate_group_size,
    validate_iterator,
    validate_throttle_interval,
    validate_throttle_per_period,
    validate_truncate_args,
)

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
    return CatchingIterator(
        iterator,
        kind,
        when,
        replacement,
        finally_raise=finally_raise,
    )


def flatten(iterator: Iterator[Iterable[T]], concurrency: int = 1) -> Iterator[T]:
    validate_iterator(iterator)
    validate_concurrency(concurrency)
    if concurrency == 1:
        return FlatteningIterator(iterator)
    else:
        return RaisingIterator(
            iter(
                ConcurrentFlatteningIterable(
                    iterator,
                    concurrency=concurrency,
                    buffer_size=concurrency,
                )
            )
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
    if size is None:
        size = cast(int, float("inf"))
    if interval is None:
        interval_seconds = float("inf")
    else:
        interval_seconds = interval.total_seconds()
    if by is not None:
        by = reraise_as(by, StopIteration, NoopStopIteration)
        return GroupingByIterator(iterator, size, interval_seconds, by)
    return GroupingIterator(iterator, size, interval_seconds)


def map(
    transformation: Callable[[T], U],
    iterator: Iterator[T],
    concurrency: int = 1,
    ordered: bool = True,
    via_processes: bool = False,
) -> Iterator[U]:
    validate_iterator(iterator)
    validate_concurrency(concurrency)
    transformation = reraise_as(transformation, StopIteration, NoopStopIteration)
    if concurrency == 1:
        return builtins.map(transformation, iterator)
    else:
        return RaisingIterator(
            iter(
                OSConcurrentMappingIterable(
                    iterator,
                    transformation,
                    concurrency=concurrency,
                    buffer_size=concurrency,
                    ordered=ordered,
                    via_processes=via_processes,
                )
            )
        )


def amap(
    transformation: Callable[[T], Coroutine[Any, Any, U]],
    iterator: Iterator[T],
    concurrency: int = 1,
    ordered: bool = True,
) -> Iterator[U]:
    validate_iterator(iterator)
    validate_concurrency(concurrency)
    return RaisingIterator(
        iter(
            AsyncConcurrentMappingIterable(
                iterator,
                transformation,
                buffer_size=concurrency,
                ordered=ordered,
            )
        )
    )


def observe(iterator: Iterator[T], what: str) -> Iterator[T]:
    validate_iterator(iterator)
    return ObservingIterator(iterator, what)


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

    restrictive_periods: List[ThrottlingPerPeriodIterator.RestrictivePeriod] = []
    if per_second < float("inf"):
        restrictive_periods.append(
            ThrottlingPerPeriodIterator.RestrictivePeriod(1, per_second)
        )
    if per_minute < float("inf"):
        restrictive_periods.append(
            ThrottlingPerPeriodIterator.RestrictivePeriod(60, per_minute)
        )
    if per_hour < float("inf"):
        restrictive_periods.append(
            ThrottlingPerPeriodIterator.RestrictivePeriod(3600, per_hour)
        )
    if restrictive_periods:
        iterator = ThrottlingPerPeriodIterator(iterator, restrictive_periods)

    if interval > datetime.timedelta(0):
        iterator = ThrottlingIntervalIterator(iterator, interval.total_seconds())
    return iterator


def truncate(
    iterator: Iterator[T],
    count: Optional[int] = None,
    when: Optional[Callable[[T], Any]] = None,
) -> Iterator[T]:
    validate_iterator(iterator)
    validate_truncate_args(count, when)
    if count is not None:
        iterator = TruncatingOnCountIterator(iterator, count)
    if when is not None:
        iterator = TruncatingOnPredicateIterator(iterator, when)
    return iterator
