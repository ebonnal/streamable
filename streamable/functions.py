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
    Union,
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
    validate_errors,
    validate_group_size,
    validate_iterator,
    # validate_not_none,
    validate_optional_count,
    validate_optional_positive_count,
    validate_optional_positive_interval,
    validate_via,
)

with suppress(ImportError):
    from typing import Literal

T = TypeVar("T")
U = TypeVar("U")


def catch(
    iterator: Iterator[T],
    errors: Union[
        Optional[Type[Exception]], Iterable[Optional[Type[Exception]]]
    ] = Exception,
    *,
    when: Optional[Callable[[Exception], Any]] = None,
    replacement: T = NO_REPLACEMENT,  # type: ignore
    finally_raise: bool = False,
) -> Iterator[T]:
    validate_iterator(iterator)
    validate_errors(errors)
    # validate_not_none(finally_raise, "finally_raise")
    if errors is None:
        return iterator
    return CatchIterator(
        iterator,
        tuple(filter(None, errors)) if isinstance(errors, Iterable) else errors,
        when=when,
        replacement=replacement,
        finally_raise=finally_raise,
    )


def distinct(
    iterator: Iterator[T],
    key: Optional[Callable[[T], Any]] = None,
    *,
    consecutive_only: bool = False,
) -> Iterator[T]:
    validate_iterator(iterator)
    # validate_not_none(consecutive_only, "consecutive_only")
    if consecutive_only:
        return ConsecutiveDistinctIterator(iterator, key)
    return DistinctIterator(iterator, key)


def flatten(iterator: Iterator[Iterable[T]], *, concurrency: int = 1) -> Iterator[T]:
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
    *,
    interval: Optional[datetime.timedelta] = None,
    by: Optional[Callable[[T], Any]] = None,
) -> Iterator[List[T]]:
    validate_iterator(iterator)
    validate_group_size(size)
    validate_optional_positive_interval(interval)
    if by is None:
        return GroupIterator(iterator, size, interval)
    return map(itemgetter(1), GroupbyIterator(iterator, by, size, interval))


def groupby(
    iterator: Iterator[T],
    key: Callable[[T], U],
    *,
    size: Optional[int] = None,
    interval: Optional[datetime.timedelta] = None,
) -> Iterator[Tuple[U, List[T]]]:
    validate_iterator(iterator)
    validate_group_size(size)
    validate_optional_positive_interval(interval)
    return GroupbyIterator(iterator, key, size, interval)


def map(
    transformation: Callable[[T], U],
    iterator: Iterator[T],
    *,
    concurrency: int = 1,
    ordered: bool = True,
    via: "Literal['thread', 'process']" = "thread",
) -> Iterator[U]:
    validate_iterator(iterator)
    # validate_not_none(transformation, "transformation")
    # validate_not_none(ordered, "ordered")
    validate_concurrency(concurrency)
    validate_via(via)
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
    *,
    concurrency: int = 1,
    ordered: bool = True,
) -> Iterator[U]:
    validate_iterator(iterator)
    # validate_not_none(transformation, "transformation")
    # validate_not_none(ordered, "ordered")
    validate_concurrency(concurrency)
    return AsyncConcurrentMapIterator(
        iterator,
        transformation,
        buffersize=concurrency,
        ordered=ordered,
    )


def observe(iterator: Iterator[T], what: str) -> Iterator[T]:
    validate_iterator(iterator)
    # validate_not_none(what, "what")
    return ObserveIterator(iterator, what)


def skip(
    iterator: Iterator[T],
    count: Optional[int] = None,
    *,
    until: Optional[Callable[[T], Any]] = None,
) -> Iterator[T]:
    validate_iterator(iterator)
    validate_optional_count(count)
    if until is not None:
        if count is not None:
            return CountAndPredicateSkipIterator(iterator, count, until)
        return PredicateSkipIterator(iterator, until)
    if count is not None:
        return CountSkipIterator(iterator, count)
    return iterator


def throttle(
    iterator: Iterator[T],
    count: Optional[int],
    *,
    per: Optional[datetime.timedelta] = None,
) -> Iterator[T]:
    validate_optional_positive_count(count)
    validate_optional_positive_interval(per, name="per")
    if count and per:
        iterator = YieldsPerPeriodThrottleIterator(iterator, count, per)
    return iterator


def truncate(
    iterator: Iterator[T],
    count: Optional[int] = None,
    *,
    when: Optional[Callable[[T], Any]] = None,
) -> Iterator[T]:
    validate_iterator(iterator)
    validate_optional_count(count)
    if count is not None:
        iterator = CountTruncateIterator(iterator, count)
    if when is not None:
        iterator = PredicateTruncateIterator(iterator, when)
    return iterator
