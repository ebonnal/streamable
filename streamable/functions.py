import builtins
import itertools
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
)

T = TypeVar("T")
U = TypeVar("U")

from streamable import _util


class _CatchingIterator(Iterator[T]):
    def __init__(
        self,
        iterator: Iterator[T],
        predicate: Callable[[Exception], Any],
        raise_at_exhaustion: bool,
    ) -> None:
        self.iterator = iterator
        self.predicate = predicate
        self.raise_at_exhaustion = raise_at_exhaustion
        self._first_catched_error: Optional[Exception] = None
        self._first_error_has_been_raised = False

    def __next__(self) -> T:
        while True:
            try:
                return next(self.iterator)
            except StopIteration:
                if (
                    self._first_catched_error is not None
                    and self.raise_at_exhaustion
                    and not self._first_error_has_been_raised
                ):
                    self._first_error_has_been_raised = True
                    raise self._first_catched_error
                raise
            except Exception as exception:
                if self.predicate(exception):
                    if self._first_catched_error is None:
                        self._first_catched_error = exception
                else:
                    raise exception


class _FlatteningIterator(Iterator[U]):
    def __init__(self, iterator: Iterator[Iterable[U]]) -> None:
        self.iterator = iterator
        self._current_iterator_elem: Iterator[U] = iter([])

    def __next__(self) -> U:
        try:
            return next(self._current_iterator_elem)
        except StopIteration:
            while True:
                elem = next(self.iterator)
                _util.validate_iterable(elem)
                self._current_iterator_elem = iter(elem)
                try:
                    return next(self._current_iterator_elem)
                except StopIteration:
                    pass


class _GroupingIterator(Iterator[List[T]]):
    def __init__(
        self, iterator: Iterator[T], size: int, seconds: float, by: Callable[[T], Any]
    ) -> None:
        self.iterator = iterator
        self.size = size
        self.seconds = seconds
        self.by = by
        self._to_be_raised: Optional[Exception] = None
        self._is_exhausted = False
        self._last_yielded_group_at = time.time()
        self._groups_by: Dict[Any, List[T]] = {}

    def _group_next_elem(self) -> None:
        elem = next(self.iterator)
        key = self.by(elem)
        if key in self._groups_by:
            self._groups_by[key].append(elem)
        else:
            self._groups_by[key] = [elem]

    def _seconds_have_elapsed(self) -> bool:
        return (time.time() - self._last_yielded_group_at) >= self.seconds

    def _pop_full_group(self) -> Optional[List[T]]:
        for key, group in self._groups_by.items():
            if len(group) >= self.size:
                return self._groups_by.pop(key)
        return None

    def _pop_first_group(self) -> List[T]:
        first_key = next(iter(self._groups_by), ...)
        return self._groups_by.pop(first_key)

    def _pop_largest_group(self) -> List[T]:
        largest_group_key: Any = next(iter(self._groups_by), ...)

        for key, group in self._groups_by.items():
            if len(group) > len(self._groups_by[largest_group_key]):
                largest_group_key = key

        return self._groups_by.pop(largest_group_key)

    def _return_group(self, group: List[T]) -> List[T]:
        self._last_yielded_group_at = time.time()
        return group

    def __next__(self) -> List[T]:
        if self._is_exhausted:
            if self._groups_by:
                return self._return_group(self._pop_first_group())
            raise StopIteration

        if self._to_be_raised:
            if self._groups_by:
                return self._return_group(self._pop_first_group())
            e = self._to_be_raised
            self._to_be_raised = None
            raise e

        try:
            self._group_next_elem()

            full_group: Optional[List[T]] = self._pop_full_group()
            while not full_group and not self._seconds_have_elapsed():
                self._group_next_elem()
                full_group = self._pop_full_group()

            if full_group:
                return self._return_group(full_group)
            return self._return_group(self._pop_largest_group())

        except StopIteration:
            self._is_exhausted = True
            return next(self)

        except Exception as e:
            if not self._groups_by:
                raise e
            self._to_be_raised = e
            return next(self)


class _LimitingIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], count: int) -> None:
        self.iterator = iterator
        self.count = count
        self._n_yields = 0

    def __next__(self):
        if self._n_yields == self.count:
            raise StopIteration()
        try:
            return next(self.iterator)
        finally:
            self._n_yields += 1


class _ObservingIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], what: str, colored: bool) -> None:
        self.iterator = iterator
        self.what = what
        self.colored = colored
        self._n_yields = 0
        self._n_errors = 0
        self._last_log_after_n_calls = 0
        self._start_time = time.time()
        _util.LOGGER.info("iteration over '%s' will be observed.", self.what)

    def _log(self) -> None:
        errors_summary = f"{self._n_errors} error"
        if self._n_errors > 1:
            errors_summary += "s"
        if self.colored and self._n_errors > 0:
            # colorize the error summary in red if any
            errors_summary = _util.bold(_util.colorize_in_red(errors_summary))

        yields_summary = f"{self._n_yields} `{self.what}` yielded"
        if self.colored:
            yields_summary = _util.bold(yields_summary)

        elapsed_time = f"after {datetime.fromtimestamp(time.time()) - datetime.fromtimestamp(self._start_time)}"

        _util.LOGGER.info("%s, %s and %s", elapsed_time, errors_summary, yields_summary)

    def _n_calls(self) -> int:
        return self._n_yields + self._n_errors

    def __next__(self) -> T:
        try:
            elem = next(self.iterator)
            self._n_yields += 1
            return elem
        except StopIteration:
            if self._n_calls() != self._last_log_after_n_calls:
                self._last_log_after_n_calls = self._n_calls()
                self._log()
            raise
        except Exception as e:
            self._n_errors += 1
            raise e
        finally:
            if self._n_calls() >= 2 * self._last_log_after_n_calls:
                self._log()
                self._last_log_after_n_calls = self._n_calls()


class _SlowingIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], frequency: float) -> None:
        self.iterator = iterator
        self.period = 1 / frequency

    def __next__(self) -> T:
        start_time = time.time()
        next_elem = next(self.iterator)
        sleep_duration = self.period - (time.time() - start_time)
        if sleep_duration > 0:
            time.sleep(sleep_duration)
        return next_elem


class _RaisingIterator(Iterator[T]):
    @dataclass
    class ExceptionContainer:
        exception: Exception

    def __init__(
        self,
        iterator: Iterator[Union[T, ExceptionContainer]],
    ) -> None:
        self.iterator = iterator

    def __next__(self) -> T:
        elem = next(self.iterator)
        if isinstance(elem, self.ExceptionContainer):
            raise elem.exception
        return elem


class _ConcurrentMappingIterable(
    Iterable[Union[U, _RaisingIterator.ExceptionContainer]]
):
    def __init__(
        self,
        iterator: Iterator[T],
        func: Callable[[T], U],
        concurrency: int,
        buffer_size: int,
    ) -> None:
        self.iterator = iterator
        self.func = func
        self.concurrency = concurrency
        self.buffer_size = buffer_size

    def __iter__(self) -> Iterator[Union[U, _RaisingIterator.ExceptionContainer]]:
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures: Deque[Future] = deque()
            # queue and yield (FIFO)
            while True:
                # queue tasks up to buffer_size
                while len(futures) < self.buffer_size:
                    try:
                        elem = next(self.iterator)
                    except StopIteration:
                        # the upstream iterator is exhausted
                        break
                    futures.append(executor.submit(self.func, elem))
                if not futures:
                    break
                try:
                    yield futures.popleft().result()
                except Exception as e:
                    yield _RaisingIterator.ExceptionContainer(e)


class _ConcurrentFlatteningIterable(
    Iterable[Union[T, _RaisingIterator.ExceptionContainer]]
):
    def __init__(
        self,
        iterables_iterator: Iterator[Iterable[T]],
        concurrency: int,
        buffer_size: int,
    ) -> None:
        self.iterables_iterator = iterables_iterator
        self.concurrency = concurrency
        self.buffer_size = buffer_size

    def __iter__(self) -> Iterator[Union[T, _RaisingIterator.ExceptionContainer]]:
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            iterator_and_future_pairs: Deque[Tuple[Iterator[T], Future]] = deque()
            # queue and yield (FIFO)
            while True:
                # queue tasks up to buffer_size
                while len(iterator_and_future_pairs) < self.buffer_size:
                    try:
                        iterable = next(self.iterables_iterator)
                    except StopIteration:
                        break
                    iterator = iter(iterable)
                    future = executor.submit(
                        cast(Callable[[Iterable[T]], T], next), iterator
                    )
                    iterator_and_future_pairs.append((iterator, future))

                if not iterator_and_future_pairs:
                    break
                iterator, future = iterator_and_future_pairs.popleft()
                try:
                    yield future.result()
                except StopIteration:
                    continue
                except Exception as e:
                    yield _RaisingIterator.ExceptionContainer(e)
                self.iterables_iterator = itertools.chain(
                    self.iterables_iterator, [iterator]
                )


# functions


class WrappedStopIteration(Exception):
    pass


def catch(
    iterator: Iterator[T],
    predicate: Callable[[Exception], Any] = bool,
    raise_at_exhaustion: bool = False,
) -> Iterator[T]:
    predicate = _util.reraise_as(
        predicate, source=StopIteration, target=WrappedStopIteration
    )
    return _CatchingIterator(
        iterator,
        predicate,
        raise_at_exhaustion=raise_at_exhaustion,
    )


def flatten(iterator: Iterator[Iterable[T]], concurrency: int = 1) -> Iterator[T]:
    _util.validate_concurrency(concurrency)
    if concurrency == 1:
        return _FlatteningIterator(iterator)
    else:
        return _RaisingIterator(
            iter(
                _ConcurrentFlatteningIterable(
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
    _util.validate_group_size(size)
    _util.validate_group_seconds(seconds)
    if by is None:
        by = lambda _: None
    else:
        by = _util.reraise_as(by, StopIteration, WrappedStopIteration)
    if size is None:
        size = cast(int, float("inf"))
    return _GroupingIterator(iterator, size, seconds, by)


def map(
    func: Callable[[T], U], iterator: Iterator[T], concurrency: int = 1
) -> Iterator[U]:
    _util.validate_concurrency(concurrency)
    func = _util.reraise_as(func, StopIteration, WrappedStopIteration)
    if concurrency == 1:
        return builtins.map(func, iterator)
    else:
        return _RaisingIterator(
            iter(
                _ConcurrentMappingIterable(
                    iterator,
                    func,
                    concurrency=concurrency,
                    buffer_size=concurrency,
                )
            )
        )


def limit(iterator: Iterator[T], count: int) -> Iterator[T]:
    _util.validate_limit_count(count)
    return _LimitingIterator(iterator, count)


def observe(iterator: Iterator[T], what: str, colored: bool = False) -> Iterator[T]:
    return _ObservingIterator(iterator, what, colored)


def slow(iterator: Iterator[T], frequency: float) -> Iterator[T]:
    _util.validate_slow_frequency(frequency)
    return _SlowingIterator(iterator, frequency)
