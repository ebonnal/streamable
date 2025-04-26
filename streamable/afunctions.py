import builtins
import datetime
from contextlib import suppress
from operator import itemgetter
from typing import (
    Any,
    Callable,
    Coroutine,
    Iterable,
    AsyncIterator,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from streamable.aiterators import (
    AsyncConcurrentMapAsyncIterator,
    AsyncMapAsyncIterator,
    CatchAsyncIterator,
    ConcurrentFlattenAsyncIterator,
    ConsecutiveDistinctAsyncIterator,
    CountAndPredicateSkipAsyncIterator,
    CountSkipAsyncIterator,
    CountTruncateAsyncIterator,
    DistinctAsyncIterator,
    FilterAsyncIterator,
    FlattenAsyncIterator,
    GroupbyAsyncIterator,
    GroupAsyncIterator,
    MapAsyncIterator,
    ObserveAsyncIterator,
    OSConcurrentMapAsyncIterator,
    PredicateSkipAsyncIterator,
    PredicateTruncateAsyncIterator,
    YieldsPerPeriodThrottleAsyncIterator,
)
from streamable.util.constants import NO_REPLACEMENT
from streamable.util.functiontools import wrap_error
from streamable.util.validationtools import (
    validate_aiterator,
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
    iterator: AsyncIterator[T],
    errors: Union[
        Optional[Type[Exception]], Iterable[Optional[Type[Exception]]]
    ] = Exception,
    *,
    when: Optional[Callable[[Exception], Any]] = None,
    replacement: T = NO_REPLACEMENT,  # type: ignore
    finally_raise: bool = False,
) -> AsyncIterator[T]:
    validate_aiterator(iterator)
    validate_errors(errors)
    # validate_not_none(finally_raise, "finally_raise")
    if errors is None:
        return iterator
    return CatchAsyncIterator(
        iterator,
        tuple(builtins.filter(None, errors))
        if isinstance(errors, Iterable)
        else errors,
        when=when,
        replacement=replacement,
        finally_raise=finally_raise,
    )


def distinct(
    iterator: AsyncIterator[T],
    key: Optional[Callable[[T], Any]] = None,
    *,
    consecutive_only: bool = False,
) -> AsyncIterator[T]:
    validate_aiterator(iterator)
    # validate_not_none(consecutive_only, "consecutive_only")
    if consecutive_only:
        return ConsecutiveDistinctAsyncIterator(iterator, key)
    return DistinctAsyncIterator(iterator, key)


def filter(
    iterator: AsyncIterator[T],
    when: Callable[[T], Any],
) -> AsyncIterator[T]:
    validate_aiterator(iterator)
    return FilterAsyncIterator(iterator, when)


def flatten(
    iterator: AsyncIterator[Iterable[T]], *, concurrency: int = 1
) -> AsyncIterator[T]:
    validate_aiterator(iterator)
    validate_concurrency(concurrency)
    if concurrency == 1:
        return FlattenAsyncIterator(iterator)
    else:
        return ConcurrentFlattenAsyncIterator(
            iterator,
            concurrency=concurrency,
            buffersize=concurrency,
        )


def group(
    iterator: AsyncIterator[T],
    size: Optional[int] = None,
    *,
    interval: Optional[datetime.timedelta] = None,
    by: Optional[Callable[[T], Any]] = None,
) -> AsyncIterator[List[T]]:
    validate_aiterator(iterator)
    validate_group_size(size)
    validate_optional_positive_interval(interval)
    if by is None:
        return GroupAsyncIterator(iterator, size, interval)
    return map(itemgetter(1), GroupbyAsyncIterator(iterator, by, size, interval))


def groupby(
    iterator: AsyncIterator[T],
    key: Callable[[T], U],
    *,
    size: Optional[int] = None,
    interval: Optional[datetime.timedelta] = None,
) -> AsyncIterator[Tuple[U, List[T]]]:
    validate_aiterator(iterator)
    validate_group_size(size)
    validate_optional_positive_interval(interval)
    return GroupbyAsyncIterator(iterator, key, size, interval)


def map(
    transformation: Callable[[T], U],
    iterator: AsyncIterator[T],
    *,
    concurrency: int = 1,
    ordered: bool = True,
    via: "Literal['thread', 'process']" = "thread",
) -> AsyncIterator[U]:
    validate_aiterator(iterator)
    # validate_not_none(transformation, "transformation")
    # validate_not_none(ordered, "ordered")
    validate_concurrency(concurrency)
    validate_via(via)
    if concurrency == 1:
        return MapAsyncIterator(iterator, transformation)
    else:
        return OSConcurrentMapAsyncIterator(
            iterator,
            transformation,
            concurrency=concurrency,
            buffersize=concurrency,
            ordered=ordered,
            via=via,
        )


def amap(
    transformation: Callable[[T], Coroutine[Any, Any, U]],
    iterator: AsyncIterator[T],
    *,
    concurrency: int = 1,
    ordered: bool = True,
) -> AsyncIterator[U]:
    validate_aiterator(iterator)
    # validate_not_none(transformation, "transformation")
    # validate_not_none(ordered, "ordered")
    validate_concurrency(concurrency)
    if concurrency == 1:
        return AsyncMapAsyncIterator(iterator, transformation)
    return AsyncConcurrentMapAsyncIterator(
        iterator,
        transformation,
        buffersize=concurrency,
        ordered=ordered,
    )


def observe(iterator: AsyncIterator[T], what: str) -> AsyncIterator[T]:
    validate_aiterator(iterator)
    # validate_not_none(what, "what")
    return ObserveAsyncIterator(iterator, what)


def skip(
    iterator: AsyncIterator[T],
    count: Optional[int] = None,
    *,
    until: Optional[Callable[[T], Any]] = None,
) -> AsyncIterator[T]:
    validate_aiterator(iterator)
    validate_optional_count(count)
    if until is not None:
        if count is not None:
            return CountAndPredicateSkipAsyncIterator(iterator, count, until)
        return PredicateSkipAsyncIterator(iterator, until)
    if count is not None:
        return CountSkipAsyncIterator(iterator, count)
    return iterator


def throttle(
    iterator: AsyncIterator[T],
    count: Optional[int],
    *,
    per: Optional[datetime.timedelta] = None,
) -> AsyncIterator[T]:
    validate_optional_positive_count(count)
    validate_optional_positive_interval(per, name="per")
    if count and per:
        iterator = YieldsPerPeriodThrottleAsyncIterator(iterator, count, per)
    return iterator


def truncate(
    iterator: AsyncIterator[T],
    count: Optional[int] = None,
    *,
    when: Optional[Callable[[T], Any]] = None,
) -> AsyncIterator[T]:
    validate_aiterator(iterator)
    validate_optional_count(count)
    if count is not None:
        iterator = CountTruncateAsyncIterator(iterator, count)
    if when is not None:
        iterator = PredicateTruncateAsyncIterator(iterator, when)
    return iterator
