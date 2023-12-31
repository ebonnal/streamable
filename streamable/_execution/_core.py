import time
from datetime import datetime
from typing import Callable, Iterable, Iterator, List, Optional, Type, TypeVar

T = TypeVar("T")
R = TypeVar("R")

from streamable import _util


class FlatteningIterator(Iterator[R]):
    def __init__(self, iterator: Iterator[Iterable[R]]) -> None:
        self.iterator = iterator
        self.current_iterator_elem: Iterator[R] = iter([])

    def __next__(self) -> R:
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


class ObservingIterator(Iterator[T]):
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


class SlowingIterator(Iterator[T]):
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


class BatchingIterator(Iterator[List[T]]):
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


class CatchingIterator(Iterator[T]):
    def __init__(
        self,
        iterator: Iterator[T],
        *classes: Type[Exception],
        when: Optional[Callable[[Exception], bool]] = None,
    ) -> None:
        self.iterator = iterator
        self.classes = classes
        self.when = when

    def __next__(self) -> T:
        try:
            return next(self.iterator)
        except StopIteration:
            raise
        except self.classes as exception:
            if self.when is None or self.when(exception):
                return next(self)  # TODO fix recursion issue
            else:
                raise exception
