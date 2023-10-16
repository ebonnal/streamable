import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from queue import Empty, Full, Queue
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


class ThreadedMappingIterator(Iterator[R]):
    _MAX_QUEUE_SIZE = 16
    def __init__(self, iterator: Iterator[T], func: Callable[[T], R], n_workers: int):
        self.iterator = iterator
        self.func = func
        self.n_workers = n_workers

    def __iter__(self):
        futures: "Queue[Future]" = Queue()
        iterator_exhausted = False
        n_yields = 0
        n_iterated_elems = 0
        with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
            while True:
                while not iterator_exhausted and executor._work_queue.qsize() < ThreadedMappingIterator._MAX_QUEUE_SIZE:
                    try:
                        elem = next(self.iterator)
                        n_iterated_elems += 1
                        futures.put(
                            executor.submit(self.func, elem)
                        )
                    except StopIteration:
                        iterator_exhausted = True
                while True:
                    if n_yields < n_iterated_elems:
                        n_yields += 1
                        logging.info(str((n_yields, "n_yields over", n_iterated_elems, "n_iterated_elems")))
                        yield futures.get().result()
                    if iterator_exhausted and n_iterated_elems == n_yields:
                        return
                    if not iterator_exhausted and executor._work_queue.qsize() < ThreadedMappingIterator._MAX_QUEUE_SIZE//2:
                        break
                

class FlatteningIterator(Iterator[R]):
    def __init__(self, iterator: Iterator[Iterator[R]]) -> None:
        self.iterator = iterator
        self.current_iterator_elem = iter([])

    def __next__(self) -> R:
        try:
            return next(self.current_iterator_elem)
        except StopIteration:
            while True:
                self.current_iterator_elem = super().__next__()
                if not isinstance(self.current_iterator_elem, Iterator):
                    raise TypeError(f"Flattened elements must be iterators, but got {type(self.current_iterator_elem)}")
                try:
                    return next(self.current_iterator_elem)
                except StopIteration:
                    pass

class LoggingIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], what: str) -> None:
        self.iterator = iterator
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


class SlowingIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], freq: float) -> None:
        self.iterator = iterator
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


class BatchingIterator(Iterator[List[T]]):
    """
    Batch an input iterator and yields its elements packed in a list when one of the following is True:
    - len(batch) == size
    - the time elapsed between the first next() call on input iterator and last received elements is grater than period
    - the next element reception thrown an exception (it is stored in self.to_be_raised and will be raised during the next call to self.__next__)
    """

    def __init__(self, iterator: Iterator[T], size: int, period: float) -> None:
        self.iterator = iterator
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

class CatchingIterator(Iterator[T]):
    def __init__(
        self, iterator: Iterator[T], classes: Tuple[Type[Exception]], ignore: bool
    ) -> None:
        self.iterator = iterator
        self.classes = classes
        self.ignore = ignore

    def __next__(self) -> T:
        try:
            return super().__next__()
        except StopIteration:
            raise
        except self.classes as e:
            if self.ignore:
                return next(self)
            else:
                return e