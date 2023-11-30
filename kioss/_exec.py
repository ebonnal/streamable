import logging
import time
from datetime import datetime
from typing import (
    Callable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

T = TypeVar("T")
R = TypeVar("R")

from kioss import _util


class IteratorWrapper(Iterator[T]):
    def __init__(self, iterator: Iterator[T]):
        self.iterator = iterator

    def __next__(self) -> T:
        return next(self.iterator)


class FlatteningIteratorWrapper(IteratorWrapper[R]):
    def __init__(self, iterator: Iterator[Iterator[R]]) -> None:
        super().__init__(iterator)
        self.current_iterator_elem = iter([])

    @staticmethod
    def _sanitize_input(expected_iterator_elem):
        _util.duck_check_type_is_iterator(expected_iterator_elem)
        return expected_iterator_elem

    def __next__(self) -> R:
        try:
            _util.duck_check_type_is_iterator(self.current_iterator_elem)
            return next(self.current_iterator_elem)
        except StopIteration:
            while True:
                elem = super().__next__()
                _util.duck_check_type_is_iterator(elem)
                self.current_iterator_elem = elem
                try:
                    return next(self.current_iterator_elem)
                except StopIteration:
                    pass


class LoggingIteratorWrapper(IteratorWrapper[T]):
    def __init__(self, iterator: Iterator[T], what: str) -> None:
        super().__init__(iterator)
        self.what = what
        self.yields_count = 0
        self.errors_count = 0
        self.last_log_at_yields_count = None
        self.start_time = time.time()
        logging.getLogger().setLevel(logging.INFO)
        logging.info("iteration over '%s' will be logged.", self.what)

    def _log(self) -> None:
        logging.info(
            "%s `%s` have been yielded in elapsed time '%s' with %s errors produced",
            self.yields_count,
            self.what,
            str(
                datetime.fromtimestamp(time.time())
                - datetime.fromtimestamp(self.start_time)
            ),
            self.errors_count,
        )

    def __next__(self) -> T:
        to_be_raised: Optional[Exception] = None
        try:
            elem = super().__next__()
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
        self.start = None
        self.yields_count = 0

    def __next__(self) -> T:
        if not self.start:
            self.start = time.time()
        while True:
            next_elem = super().__next__()
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
        self._to_be_raised: Exception = None
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
            batch = [super().__next__()]
            while len(batch) < self.size and (time.time() - start_time) < self.period:
                batch.append(super().__next__())
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
        classes: Tuple[Type[Exception]],
        when: Optional[Callable[[Exception], bool]],
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
