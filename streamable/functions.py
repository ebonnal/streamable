import builtins
import datetime
from contextlib import suppress
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
    ByKeyGroupingIterator,
    CatchingIterator,
    ConcurrentFlatteningIterable,
    CountTruncatingIterator,
    FlatteningIterator,
    GroupingIterator,
    IntervalThrottlingIterator,
    ObservingIterator,
    OSConcurrentMappingIterable,
    PredicateTruncatingIterator,
    RaisingIterator,
    SkipIterator,
    YieldsPerPeriodThrottlingIterator,
)
from streamable.util.constants import NO_REPLACEMENT
from streamable.util.exceptions import NoopStopIteration
from streamable.util.functiontools import catch_and_raise_as
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
        by = catch_and_raise_as(by, StopIteration, NoopStopIteration)
        return ByKeyGroupingIterator(iterator, size, interval_seconds, by)
    return GroupingIterator(iterator, size, interval_seconds)


def map(
    transformation: Callable[[T], U],
    iterator: Iterator[T],
    concurrency: int = 1,
    ordered: bool = True,
    via: "Literal['thread', 'process']" = "thread",
) -> Iterator[U]:
    validate_iterator(iterator)
    validate_concurrency(concurrency)
    transformation = catch_and_raise_as(
        transformation, StopIteration, NoopStopIteration
    )
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
                    via=via,
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
            iterator = YieldsPerPeriodThrottlingIterator(iterator, per_period, period)

    if interval > datetime.timedelta(0):
        iterator = IntervalThrottlingIterator(iterator, interval)
    return iterator


def truncate(
    iterator: Iterator[T],
    count: Optional[int] = None,
    when: Optional[Callable[[T], Any]] = None,
) -> Iterator[T]:
    validate_iterator(iterator)
    validate_truncate_args(count, when)
    if count is not None:
        iterator = CountTruncatingIterator(iterator, count)
    if when is not None:
        iterator = PredicateTruncatingIterator(iterator, when)
    return iterator
