import builtins
import datetime
from contextlib import suppress
from operator import itemgetter
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Coroutine,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from streamable.aiterators import (
    ACatchAsyncIterator,
    ADistinctAsyncIterator,
    AFilterAsyncIterator,
    AFlattenAsyncIterator,
    AGroupbyAsyncIterator,
    AMapAsyncIterator,
    ConcurrentAFlattenAsyncIterator,
    ConcurrentAMapAsyncIterator,
    ConcurrentFlattenAsyncIterator,
    ConcurrentMapAsyncIterator,
    ConsecutiveADistinctAsyncIterator,
    CountAndPredicateASkipAsyncIterator,
    CountSkipAsyncIterator,
    CountTruncateAsyncIterator,
    FlattenAsyncIterator,
    GroupAsyncIterator,
    ObserveAsyncIterator,
    PredicateASkipAsyncIterator,
    PredicateATruncateAsyncIterator,
    YieldsPerPeriodThrottleAsyncIterator,
)
from streamable.util.constants import NO_REPLACEMENT
from streamable.util.functiontools import asyncify
from streamable.util.validationtools import (
    validate_aiterator,
    validate_concurrency,
    validate_errors,
    validate_group_size,
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
    aiterator: AsyncIterator[T],
    errors: Union[
        Optional[Type[Exception]], Iterable[Optional[Type[Exception]]]
    ] = Exception,
    *,
    when: Optional[Callable[[Exception], Any]] = None,
    replacement: T = NO_REPLACEMENT,  # type: ignore
    finally_raise: bool = False,
) -> AsyncIterator[T]:
    return acatch(
        aiterator,
        errors,
        when=asyncify(when) if when else None,
        replacement=replacement,
        finally_raise=finally_raise,
    )


def acatch(
    aiterator: AsyncIterator[T],
    errors: Union[
        Optional[Type[Exception]], Iterable[Optional[Type[Exception]]]
    ] = Exception,
    *,
    when: Optional[Callable[[Exception], Coroutine[Any, Any, Any]]] = None,
    replacement: T = NO_REPLACEMENT,  # type: ignore
    finally_raise: bool = False,
) -> AsyncIterator[T]:
    validate_aiterator(aiterator)
    validate_errors(errors)
    # validate_not_none(finally_raise, "finally_raise")
    if errors is None:
        return aiterator
    return ACatchAsyncIterator(
        aiterator,
        tuple(builtins.filter(None, errors))
        if isinstance(errors, Iterable)
        else errors,
        when=when,
        replacement=replacement,
        finally_raise=finally_raise,
    )


def distinct(
    aiterator: AsyncIterator[T],
    key: Optional[Callable[[T], Any]] = None,
    *,
    consecutive_only: bool = False,
) -> AsyncIterator[T]:
    return adistinct(
        aiterator,
        asyncify(key) if key else None,
        consecutive_only=consecutive_only,
    )


def adistinct(
    aiterator: AsyncIterator[T],
    key: Optional[Callable[[T], Coroutine[Any, Any, Any]]] = None,
    *,
    consecutive_only: bool = False,
) -> AsyncIterator[T]:
    validate_aiterator(aiterator)
    # validate_not_none(consecutive_only, "consecutive_only")
    if consecutive_only:
        return ConsecutiveADistinctAsyncIterator(aiterator, key)
    return ADistinctAsyncIterator(aiterator, key)


def filter(
    aiterator: AsyncIterator[T],
    when: Callable[[T], Any],
) -> AsyncIterator[T]:
    return afilter(aiterator, asyncify(when))


def afilter(
    aiterator: AsyncIterator[T],
    when: Callable[[T], Any],
) -> AsyncIterator[T]:
    validate_aiterator(aiterator)
    return AFilterAsyncIterator(aiterator, when)


def flatten(
    aiterator: AsyncIterator[Iterable[T]], *, concurrency: int = 1
) -> AsyncIterator[T]:
    validate_aiterator(aiterator)
    validate_concurrency(concurrency)
    if concurrency == 1:
        return FlattenAsyncIterator(aiterator)
    else:
        return ConcurrentFlattenAsyncIterator(
            aiterator,
            concurrency=concurrency,
            buffersize=concurrency,
        )


def aflatten(
    aiterator: AsyncIterator[AsyncIterable[T]], *, concurrency: int = 1
) -> AsyncIterator[T]:
    validate_aiterator(aiterator)
    validate_concurrency(concurrency)
    if concurrency == 1:
        return AFlattenAsyncIterator(aiterator)
    else:
        return ConcurrentAFlattenAsyncIterator(
            aiterator,
            concurrency=concurrency,
            buffersize=concurrency,
        )


def group(
    aiterator: AsyncIterator[T],
    size: Optional[int] = None,
    *,
    interval: Optional[datetime.timedelta] = None,
    by: Optional[Callable[[T], Any]] = None,
) -> AsyncIterator[List[T]]:
    return agroup(
        aiterator,
        size,
        interval=interval,
        by=asyncify(by) if by else None,
    )


def agroup(
    aiterator: AsyncIterator[T],
    size: Optional[int] = None,
    *,
    interval: Optional[datetime.timedelta] = None,
    by: Optional[Callable[[T], Coroutine[Any, Any, Any]]] = None,
) -> AsyncIterator[List[T]]:
    validate_aiterator(aiterator)
    validate_group_size(size)
    validate_optional_positive_interval(interval)
    if by is None:
        return GroupAsyncIterator(aiterator, size, interval)
    return map(itemgetter(1), AGroupbyAsyncIterator(aiterator, by, size, interval))


def groupby(
    aiterator: AsyncIterator[T],
    key: Callable[[T], U],
    *,
    size: Optional[int] = None,
    interval: Optional[datetime.timedelta] = None,
) -> AsyncIterator[Tuple[U, List[T]]]:
    return agroupby(aiterator, asyncify(key), size=size, interval=interval)


def agroupby(
    aiterator: AsyncIterator[T],
    key: Callable[[T], Coroutine[Any, Any, U]],
    *,
    size: Optional[int] = None,
    interval: Optional[datetime.timedelta] = None,
) -> AsyncIterator[Tuple[U, List[T]]]:
    validate_aiterator(aiterator)
    validate_group_size(size)
    validate_optional_positive_interval(interval)
    return AGroupbyAsyncIterator(aiterator, key, size, interval)


def map(
    transformation: Callable[[T], U],
    aiterator: AsyncIterator[T],
    *,
    concurrency: int = 1,
    ordered: bool = True,
    via: "Literal['thread', 'process']" = "thread",
) -> AsyncIterator[U]:
    validate_aiterator(aiterator)
    # validate_not_none(transformation, "transformation")
    # validate_not_none(ordered, "ordered")
    validate_concurrency(concurrency)
    validate_via(via)
    if concurrency == 1:
        return amap(asyncify(transformation), aiterator)
    else:
        return ConcurrentMapAsyncIterator(
            aiterator,
            transformation,
            concurrency=concurrency,
            buffersize=concurrency,
            ordered=ordered,
            via=via,
        )


def amap(
    transformation: Callable[[T], Coroutine[Any, Any, U]],
    aiterator: AsyncIterator[T],
    *,
    concurrency: int = 1,
    ordered: bool = True,
) -> AsyncIterator[U]:
    validate_aiterator(aiterator)
    # validate_not_none(transformation, "transformation")
    # validate_not_none(ordered, "ordered")
    validate_concurrency(concurrency)
    if concurrency == 1:
        return AMapAsyncIterator(aiterator, transformation)
    return ConcurrentAMapAsyncIterator(
        aiterator,
        transformation,
        buffersize=concurrency,
        ordered=ordered,
    )


def observe(aiterator: AsyncIterator[T], what: str) -> AsyncIterator[T]:
    validate_aiterator(aiterator)
    # validate_not_none(what, "what")
    return ObserveAsyncIterator(aiterator, what)


def skip(
    aiterator: AsyncIterator[T],
    count: Optional[int] = None,
    *,
    until: Optional[Callable[[T], Any]] = None,
) -> AsyncIterator[T]:
    return askip(
        aiterator,
        count,
        until=asyncify(until) if until else None,
    )


def askip(
    aiterator: AsyncIterator[T],
    count: Optional[int] = None,
    *,
    until: Optional[Callable[[T], Coroutine[Any, Any, Any]]] = None,
) -> AsyncIterator[T]:
    validate_aiterator(aiterator)
    validate_optional_count(count)
    if until is not None:
        if count is not None:
            return CountAndPredicateASkipAsyncIterator(aiterator, count, until)
        return PredicateASkipAsyncIterator(aiterator, until)
    if count is not None:
        return CountSkipAsyncIterator(aiterator, count)
    return aiterator


def throttle(
    aiterator: AsyncIterator[T],
    count: Optional[int],
    *,
    per: Optional[datetime.timedelta] = None,
) -> AsyncIterator[T]:
    validate_optional_positive_count(count)
    validate_optional_positive_interval(per, name="per")
    if count and per:
        aiterator = YieldsPerPeriodThrottleAsyncIterator(aiterator, count, per)
    return aiterator


def truncate(
    aiterator: AsyncIterator[T],
    count: Optional[int] = None,
    *,
    when: Optional[Callable[[T], Any]] = None,
) -> AsyncIterator[T]:
    return atruncate(
        aiterator,
        count,
        when=asyncify(when) if when else None,
    )


def atruncate(
    aiterator: AsyncIterator[T],
    count: Optional[int] = None,
    *,
    when: Optional[Callable[[T], Coroutine[Any, Any, Any]]] = None,
) -> AsyncIterator[T]:
    validate_aiterator(aiterator)
    validate_optional_count(count)
    if count is not None:
        aiterator = CountTruncateAsyncIterator(aiterator, count)
    if when is not None:
        aiterator = PredicateATruncateAsyncIterator(aiterator, when)
    return aiterator
