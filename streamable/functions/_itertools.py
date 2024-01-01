import builtins
import itertools
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from queue import Queue
from typing import (
    Callable,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

T = TypeVar("T")
U = TypeVar("U")

from streamable import _util


class _ObservingIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], what: str, colored: bool) -> None:
        self.iterator = iterator
        self.what = what
        self.colored = colored
        self.yields_count = 0
        self.errors_count = 0
        self.last_log_at_yields_count: Optional[int] = None
        self.start_time = time.time()
        _util.LOGGER.info("iteration over '%s' will be logged.", self.what)

    def _log(self) -> None:
        errors_summary = f"with {self.errors_count} error{'s' if self.errors_count > 1 else ''} produced"
        if self.colored and self.errors_count > 0:
            errors_summary = _util.bold(_util.colorize_in_red(errors_summary))
        yields_summary = f"{self.yields_count} {self.what} have been yielded"
        if self.colored:
            yields_summary = _util.bold(yields_summary)
        elapsed_time = f"in elapsed time {datetime.fromtimestamp(time.time()) - datetime.fromtimestamp(self.start_time)}"
        _util.LOGGER.info("%s, %s, %s", yields_summary, elapsed_time, errors_summary)

    def __next__(self) -> T:
        to_be_raised: Optional[Exception] = None
        try:
            elem = next(self.iterator)
        except StopIteration:
            if self.yields_count != self.last_log_at_yields_count:
                self._log()
                self.last_log_at_yields_count = self.yields_count
            raise
        except Exception as e:
            to_be_raised = e
            self.errors_count += 1

        self.yields_count += 1
        if (
            self.last_log_at_yields_count is None
            or self.yields_count == 2 * self.last_log_at_yields_count
        ):
            self._log()
            self.last_log_at_yields_count = self.yields_count
        if to_be_raised:
            raise to_be_raised
        return elem


def observe(iterator: Iterator[T], what: str, colored: bool = False) -> Iterator[T]:
    return _ObservingIterator(iterator, what, colored)


class _SlowingIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], frequency: float) -> None:
        self.iterator = iterator
        self.frequency = frequency
        self.start: Optional[float] = None
        self.yields_count = 0

    def __next__(self) -> T:
        if not self.start:
            self.start = time.time()
        while True:
            next_elem = next(self.iterator)
            while self.yields_count > (time.time() - self.start) * self.frequency:
                time.sleep(0.001)
            self.yields_count += 1
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
        *classes: Type[Exception],
        when: Optional[Callable[[Exception], bool]],
        raise_at_exhaustion: bool,
    ) -> None:
        self.iterator = iterator
        self.classes = classes
        self.when = when
        self.raise_at_exhaustion = raise_at_exhaustion
        self.first_catched_error: Optional[Exception] = None
        self.first_error_has_been_raised = False

    def __next__(self) -> T:
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
        except self.classes as exception:
            if self.when is None or self.when(exception):
                if self.first_catched_error is None:
                    self.first_catched_error = exception
                return next(self)  # TODO fix recursion issue
            else:
                raise exception


def catch(
    iterator: Iterator[T],
    *classes: Type[Exception],
    when: Optional[Callable[[Exception], bool]] = None,
    raise_at_exhaustion: bool = False,
) -> Iterator[T]:
    if when is not None:
        when = _util.map_exception(when, source=StopIteration, target=RuntimeError)
    return _CatchingIterator(
        iterator,
        *classes,
        when=when,
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


_CONCURRENCY_BUFFER_SIZE_FACTOR = 3


class _ConcurrentMappingIterable(
    Iterable[Union[U, _RaisingIterator.ExceptionContainer]]
):
    def __init__(self, iterator: Iterator[T], func: Callable[[T], U], concurrency: int):
        self.iterator = iterator
        self.func = func
        self.concurrency = concurrency
        self.buffer_size = concurrency * _CONCURRENCY_BUFFER_SIZE_FACTOR

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
    if concurrency == 1:
        return builtins.map(func, iterator)
    else:
        return _RaisingIterator(
            iter(_ConcurrentMappingIterable(iterator, func, concurrency=concurrency))
        )


class _ConcurrentFlatteningIterable(
    Iterable[Union[T, _RaisingIterator.ExceptionContainer]]
):
    def __init__(self, iterables_iterator: Iterator[Iterable[T]], concurrency: int):
        self.iterables_iterator = iterables_iterator
        self.concurrency = concurrency
        self.buffer_size = concurrency * _CONCURRENCY_BUFFER_SIZE_FACTOR

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
            iter(_ConcurrentFlatteningIterable(iterator, concurrency=concurrency))
        )
