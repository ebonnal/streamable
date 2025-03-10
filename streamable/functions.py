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
)

from streamable.iterators import (
    AsyncConcurrentMapIterator,
    CatchIterator,
    ConcurrentFlattenIterator,
    ConsecutiveDistinctIterator,
    CountAndPredicateSkipIterator,
    CountSkipIterator,
    CountTruncateIterator,
    DistinctIterator,
    FlattenIterator,
    GroupbyIterator,
    GroupIterator,
    ObserveIterator,
    OSConcurrentMapIterator,
    PredicateSkipIterator,
    PredicateTruncateIterator,
    YieldsPerPeriodThrottleIterator,
)
from streamable.util.constants import NO_REPLACEMENT
from streamable.util.functiontools import wrap_error
from streamable.util.validationtools import (
    validate_concurrency,
    validate_group_interval,
    validate_group_size,
    validate_iterable,
    validate_optional_count,
    validate_optional_positive_count,
    validate_throttle_per,
)

with suppress(ImportError):
    from typing import Literal

T = TypeVar("T")
U = TypeVar("U")


def catch(
    iterable: Iterable[T],
    error_type: Optional[Type[Exception]],
    *others: Optional[Type[Exception]],
    when: Optional[Callable[[Exception], Any]] = None,
    replacement: T = NO_REPLACEMENT,  # type: ignore
    finally_raise: bool = False,
) -> Iterator[T]:
    validate_iterable(iterable)
    return CatchIterator(
        iter(iterable),
        (error_type, *others),
        when,
        replacement,
        finally_raise,
    )


def distinct(
    iterable: Iterable[T],
    key: Optional[Callable[[T], Any]] = None,
    consecutive_only: bool = False,
) -> Iterator[T]:
    validate_iterable(iterable)
    if consecutive_only:
        return ConsecutiveDistinctIterator(iter(iterable), key)
    return DistinctIterator(iter(iterable), key)


def flatten(iterable: Iterable[Iterable[T]], concurrency: int = 1) -> Iterator[T]:
    validate_iterable(iterable)
    validate_concurrency(concurrency)
    if concurrency == 1:
        return FlattenIterator(iter(iterable))
    else:
        return ConcurrentFlattenIterator(
            iter(iterable),
            concurrency=concurrency,
            buffersize=concurrency,
        )


def group(
    iterable: Iterable[T],
    size: Optional[int] = None,
    interval: Optional[datetime.timedelta] = None,
    by: Optional[Callable[[T], Any]] = None,
) -> Iterator[List[T]]:
    validate_iterable(iterable)
    validate_group_size(size)
    validate_group_interval(interval)
    if by is None:
        return GroupIterator(iter(iterable), size, interval)
    return map(itemgetter(1), GroupbyIterator(iter(iterable), by, size, interval))


def groupby(
    iterable: Iterable[T],
    key: Callable[[T], U],
    size: Optional[int] = None,
    interval: Optional[datetime.timedelta] = None,
) -> Iterator[Tuple[U, List[T]]]:
    validate_iterable(iterable)
    validate_group_size(size)
    validate_group_interval(interval)
    return GroupbyIterator(iter(iterable), key, size, interval)


def map(
    transformation: Callable[[T], U],
    iterable: Iterable[T],
    concurrency: int = 1,
    ordered: bool = True,
    via: "Literal['thread', 'process']" = "thread",
) -> Iterator[U]:
    validate_iterable(iterable)
    validate_concurrency(concurrency)
    if concurrency == 1:
        return builtins.map(wrap_error(transformation, StopIteration), iterable)
    else:
        return OSConcurrentMapIterator(
            iter(iterable),
            transformation,
            concurrency=concurrency,
            buffersize=concurrency,
            ordered=ordered,
            via=via,
        )


def amap(
    transformation: Callable[[T], Coroutine[Any, Any, U]],
    iterable: Iterable[T],
    concurrency: int = 1,
    ordered: bool = True,
) -> Iterator[U]:
    validate_iterable(iterable)
    validate_concurrency(concurrency)
    return AsyncConcurrentMapIterator(
        iter(iterable),
        transformation,
        buffersize=concurrency,
        ordered=ordered,
    )


def observe(iterable: Iterable[T], what: str) -> Iterator[T]:
    validate_iterable(iterable)
    return ObserveIterator(iter(iterable), what)


def skip(
    iterable: Iterable[T],
    count: Optional[int] = None,
    until: Optional[Callable[[T], Any]] = None,
) -> Iterator[T]:
    validate_iterable(iterable)
    validate_optional_count(count)
    if until is not None:
        if count is not None:
            return CountAndPredicateSkipIterator(iter(iterable), count, until)
        return PredicateSkipIterator(iter(iterable), until)
    if count is not None:
        return CountSkipIterator(iter(iterable), count)
    return iter(iterable)


def throttle(
    iterable: Iterable[T],
    count: Optional[int],
    per: Optional[datetime.timedelta] = None,
) -> Iterator[T]:
    validate_optional_positive_count(count)
    validate_throttle_per(per)
    iterator = iter(iterable)
    if count and per:
        iterator = YieldsPerPeriodThrottleIterator(iterator, count, per)
    return iterator


def truncate(
    iterable: Iterable[T],
    count: Optional[int] = None,
    when: Optional[Callable[[T], Any]] = None,
) -> Iterator[T]:
    validate_iterable(iterable)
    validate_optional_count(count)
    iterator = iter(iterable)
    if count is not None:
        iterator = CountTruncateIterator(iterator, count)
    if when is not None:
        iterator = PredicateTruncateIterator(iterator, when)
    return iterator
