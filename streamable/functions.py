import builtins
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
    RaisingIterator,
    ThreadConcurrentMappingIterable,
    ThrottlingIterator,
    TruncatingOnCountIterator,
    TruncatingOnPredicateIterator,
)

T = TypeVar("T")
U = TypeVar("U")

from streamable import util
from streamable.util import NoopStopIteration


def catch(
    iterator: Iterator[T],
    kind: Type[Exception] = Exception,
    when: Callable[[Exception], Any] = bool,
    finally_raise: bool = False,
) -> Iterator[T]:
    util.validate_iterator(iterator)
    return CatchingIterator(
        iterator,
        kind,
        when,
        finally_raise=finally_raise,
    )


def flatten(iterator: Iterator[Iterable[T]], concurrency: int = 1) -> Iterator[T]:
    util.validate_iterator(iterator)
    util.validate_concurrency(concurrency)
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
    seconds: float = float("inf"),
    by: Optional[Callable[[T], Any]] = None,
) -> Iterator[List[T]]:
    util.validate_iterator(iterator)
    util.validate_group_size(size)
    util.validate_group_seconds(seconds)
    if size is None:
        size = cast(int, float("inf"))
    if by is not None:
        by = util.reraise_as(by, StopIteration, NoopStopIteration)
        return GroupingByIterator(iterator, size, seconds, by)
    return GroupingIterator(iterator, size, seconds)


def map(
    transformation: Callable[[T], U], iterator: Iterator[T], concurrency: int = 1
) -> Iterator[U]:
    util.validate_iterator(iterator)
    util.validate_concurrency(concurrency)
    transformation = util.reraise_as(transformation, StopIteration, NoopStopIteration)
    if concurrency == 1:
        return builtins.map(transformation, iterator)
    else:
        return RaisingIterator(
            iter(
                ThreadConcurrentMappingIterable(
                    iterator,
                    transformation,
                    concurrency=concurrency,
                    buffer_size=concurrency,
                )
            )
        )


def amap(
    transformation: Callable[[T], Coroutine[Any, Any, U]],
    iterator: Iterator[T],
    concurrency: int = 1,
) -> Iterator[U]:
    util.validate_iterator(iterator)
    util.validate_concurrency(concurrency)
    return RaisingIterator(
        iter(
            AsyncConcurrentMappingIterable(
                iterator,
                transformation,
                buffer_size=concurrency,
            )
        )
    )


def observe(iterator: Iterator[T], what: str) -> Iterator[T]:
    util.validate_iterator(iterator)
    return ObservingIterator(iterator, what)


def throttle(iterator: Iterator[T], per_second: float) -> Iterator[T]:
    util.validate_iterator(iterator)
    util.validate_throttle_per_second(per_second)
    return ThrottlingIterator(iterator, per_second)


def truncate(
    iterator: Iterator[T],
    count: Optional[int] = None,
    when: Optional[Callable[[T], Any]] = None,
) -> Iterator[T]:
    util.validate_iterator(iterator)
    util.validate_truncate_args(count, when)
    if count is not None:
        iterator = TruncatingOnCountIterator(iterator, count)
    if when is not None:
        iterator = TruncatingOnPredicateIterator(iterator, when)
    return iterator
