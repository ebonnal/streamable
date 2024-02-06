import builtins
import itertools
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from queue import Queue
from typing import (
    Any,
    Callable,
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


class LimitingIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], count: int) -> None:
        self.iterator = iterator
        self.count = count
        self.n_yields = 0

    def __next__(self):
        if self.n_yields == self.count:
            raise StopIteration()
        try:
            return next(self.iterator)
        finally:
            self.n_yields += 1


def limit(iterator: Iterator[T], count: int) -> Iterator[T]:
    _util.validate_limit_count(count)
    return LimitingIterator(iterator, count)


class _ObservingIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], what: str, colored: bool) -> None:
        self.iterator = iterator
        self.what = what
        self.colored = colored
        self.n_yields = 0
        self.n_errors = 0
        self.last_log_after_n_calls = 0
        self.start_time = time.time()
        _util.LOGGER.info("iteration over '%s' will be observed.", self.what)

    def _log(self) -> None:
        errors_summary = f"{self.n_errors} error"
        if self.n_errors > 1:
            errors_summary += "s"
        if self.colored and self.n_errors > 0:
            # colorize the error summary in red if any
            errors_summary = _util.bold(_util.colorize_in_red(errors_summary))

        yields_summary = f"{self.n_yields} `{self.what}` yielded"
        if self.colored:
            yields_summary = _util.bold(yields_summary)

        elapsed_time = f"after {datetime.fromtimestamp(time.time()) - datetime.fromtimestamp(self.start_time)}"

        _util.LOGGER.info("%s, %s and %s", elapsed_time, errors_summary, yields_summary)

    def n_calls(self) -> int:
        return self.n_yields + self.n_errors

    def __next__(self) -> T:
        try:
            elem = next(self.iterator)
            self.n_yields += 1
            return elem
        except StopIteration:
            if self.n_calls() != self.last_log_after_n_calls:
                self.last_log_after_n_calls = self.n_calls()
                self._log()
            raise
        except Exception as e:
            self.n_errors += 1
            raise e
        finally:
            if self.n_calls() >= 2 * self.last_log_after_n_calls:
                self._log()
                self.last_log_after_n_calls = self.n_calls()


def observe(iterator: Iterator[T], what: str, colored: bool = False) -> Iterator[T]:
    return _ObservingIterator(iterator, what, colored)


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


def slow(iterator: Iterator[T], frequency: float) -> Iterator[T]:
    _util.validate_slow_frequency(frequency)
    return _SlowingIterator(iterator, frequency)


class _BatchingIterator(Iterator[List[T]]):
    def __init__(self, iterator: Iterator[T], size: int, seconds: float) -> None:
        self.iterator = iterator
        self.size = size
        self.seconds = seconds
        self._to_be_raised: Optional[Exception] = None
        self._is_exhausted = False
        self.last_yielded_batch_at = time.time()

    def __next__(self) -> List[T]:
        if self._is_exhausted:
            raise StopIteration
        if self._to_be_raised:
            e = self._to_be_raised
            self._to_be_raised = None
            raise e
        batch = None
        try:
            batch = [next(self.iterator)]
            while (
                len(batch) < self.size
                and (time.time() - self.last_yielded_batch_at) < self.seconds
            ):
                batch.append(next(self.iterator))
            self.last_yielded_batch_at = time.time()
            return batch
        except StopIteration:
            self._is_exhausted = True
            if batch:
                self.last_yielded_batch_at = time.time()
                return batch
            raise
        except Exception as e:
            if batch:
                self._to_be_raised = e
                self.last_yielded_batch_at = time.time()
                return batch
            raise e


def batch(
    iterator: Iterator[T], size: int, seconds: float = float("inf")
) -> Iterator[List[T]]:
    _util.validate_batch_size(size)
    _util.validate_batch_seconds(seconds)
    return _BatchingIterator(iterator, size, seconds)


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
        self.first_catched_error: Optional[Exception] = None
        self.first_error_has_been_raised = False

    def __next__(self) -> T:
        while True:
            try:
                return next(self.iterator)
            except StopIteration:
                if (
                    self.first_catched_error is not None
                    and self.raise_at_exhaustion
                    and not self.first_error_has_been_raised
                ):
                    self.first_error_has_been_raised = True
                    raise self.first_catched_error
                raise
            except Exception as exception:
                if self.predicate(exception):
                    if self.first_catched_error is None:
                        self.first_catched_error = exception
                else:
                    raise exception


def catch(
    iterator: Iterator[T],
    predicate: Callable[[Exception], Any] = bool,
    raise_at_exhaustion: bool = False,
) -> Iterator[T]:
    predicate = _util.map_exception(
        predicate, source=StopIteration, target=RuntimeError
    )
    return _CatchingIterator(
        iterator,
        predicate,
        raise_at_exhaustion=raise_at_exhaustion,
    )


class _RaisingIterator(Iterator[T]):
    @dataclass
    class ExceptionContainer:
        exception: Exception

    def __init__(
        self,
        iterator: Iterator[Union[T, ExceptionContainer]],
    ):
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
    ):
        self.iterator = iterator
        self.func = func
        self.concurrency = concurrency
        self.buffer_size = buffer_size

    def __iter__(self) -> Iterator[Union[U, _RaisingIterator.ExceptionContainer]]:
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures: "Queue[Future]" = Queue(maxsize=self.buffer_size)
            # queue and yield (FIFO)
            while True:
                # queue tasks up to queue's maxsize
                while not futures.full():
                    try:
                        elem = next(self.iterator)
                    except StopIteration:
                        # the upstream iterator is exhausted
                        break
                    futures.put(executor.submit(self.func, elem))
                if futures.empty():
                    break
                try:
                    yield futures.get().result()
                except Exception as e:
                    yield _RaisingIterator.ExceptionContainer(e)


def map(
    func: Callable[[T], U], iterator: Iterator[T], concurrency: int = 1
) -> Iterator[U]:
    _util.validate_concurrency(concurrency)
    func = _util.map_exception(func, StopIteration, RuntimeError)
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


class _ConcurrentFlatteningIterable(
    Iterable[Union[T, _RaisingIterator.ExceptionContainer]]
):
    def __init__(
        self,
        iterables_iterator: Iterator[Iterable[T]],
        concurrency: int,
        buffer_size: int,
    ):
        self.iterables_iterator = iterables_iterator
        self.concurrency = concurrency
        self.buffer_size = buffer_size

    def __iter__(self) -> Iterator[Union[T, _RaisingIterator.ExceptionContainer]]:
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            iterator_and_future_pairs: "Queue[Tuple[Iterator[T], Future]]" = Queue(
                maxsize=self.buffer_size
            )
            # queue and yield (FIFO)
            while True:
                # queue tasks up to queue's maxsize
                while not iterator_and_future_pairs.full():
                    try:
                        iterable = next(self.iterables_iterator)
                    except StopIteration:
                        break
                    iterator = iter(iterable)
                    future = executor.submit(
                        cast(Callable[[Iterable[T]], T], next), iterator
                    )
                    iterator_and_future_pairs.put((iterator, future))

                if iterator_and_future_pairs.empty():
                    break
                iterator, future = iterator_and_future_pairs.get()
                try:
                    yield future.result()
                except StopIteration:
                    continue
                except Exception as e:
                    yield _RaisingIterator.ExceptionContainer(e)
                self.iterables_iterator = itertools.chain(
                    self.iterables_iterator, [iterator]
                )


class _FlatteningIterator(Iterator[U]):
    def __init__(self, iterator: Iterator[Iterable[U]]) -> None:
        self.iterator = iterator
        self.current_iterator_elem: Iterator[U] = iter([])

    def __next__(self) -> U:
        try:
            return next(self.current_iterator_elem)
        except StopIteration:
            while True:
                elem = next(self.iterator)
                _util.validate_iterable(elem)
                self.current_iterator_elem = iter(elem)
                try:
                    return next(self.current_iterator_elem)
                except StopIteration:
                    pass


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
