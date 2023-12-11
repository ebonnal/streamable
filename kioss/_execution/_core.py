import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Callable, Iterable, Iterator, List, Optional, Type, TypeVar

T = TypeVar("T")
R = TypeVar("R")

from kioss import _util


class IteratorWrapper(Iterator[T], ABC):
    def __init__(self, iterator: Iterator):
        self.iterator = iterator

    @abstractmethod
    def __next__(self) -> T:
        ...


class FlatteningIteratorWrapper(IteratorWrapper[R]):
    def __init__(self, iterator: Iterator[Iterable[R]]) -> None:
        super().__init__(iterator)
        self.current_iterator_elem: Iterator[R] = iter([])

    def __next__(self) -> R:
        try:
            return next(self.current_iterator_elem)
        except StopIteration:
            while True:
                elem = next(self.iterator)
                _util.ducktype_assert_iterable(elem)
                self.current_iterator_elem = iter(elem)
                try:
                    return next(self.current_iterator_elem)
                except StopIteration:
                    pass


class LoggingIteratorWrapper(IteratorWrapper[T]):
    def __init__(self, iterator: Iterator[T], what: str, colored: bool) -> None:
        super().__init__(iterator)
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


class SlowingIteratorWrapper(IteratorWrapper[T]):
    def __init__(self, iterator: Iterator[T], freq: float) -> None:
        super().__init__(iterator)
        self.freq = freq
        self.start: Optional[float] = None
        self.yields_count = 0

    def __next__(self) -> T:
        if not self.start:
            self.start = time.time()
        while True:
            next_elem = next(self.iterator)
            while self.yields_count > (time.time() - self.start) * self.freq:
                time.sleep(0.001)
            self.yields_count += 1
            return next_elem


class BatchingIteratorWrapper(IteratorWrapper[List[T]]):
    """
    Batch an input iterator and yields its elements packed in a list when one of the following is True:
    - len(batch) == size
    - the time elapsed between the first next() call on input iterator and last received elements is grater than period
    - the next element reception thrown an exception (it is stored in self.to_be_raised and will be raised during the next call to self.__next__)
    """

    def __init__(self, iterator: Iterator[T], size: int, period: float) -> None:
        super().__init__(iterator)
        self.size = size
        self.period = period
        self._to_be_raised: Optional[Exception] = None
        self._is_exhausted = False

    def __next__(self) -> List[T]:
        if self._is_exhausted:
            raise StopIteration
        if self._to_be_raised:
            e = self._to_be_raised
            self._to_be_raised = None
            raise e
        start_time = time.time()
        batch = None
        try:
            batch = [next(self.iterator)]
            while len(batch) < self.size and (time.time() - start_time) < self.period:
                batch.append(next(self.iterator))
            return batch
        except StopIteration:
            self._is_exhausted = True
            if batch:
                return batch
            raise
        except Exception as e:
            if batch:
                self._to_be_raised = e
                return batch
            raise e


class CatchingIteratorWrapper(IteratorWrapper[T]):
    def __init__(
        self,
        iterator: Iterator[T],
        *classes: Type[Exception],
        when: Optional[Callable[[Exception], bool]] = None,
    ) -> None:
        super().__init__(iterator)
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
