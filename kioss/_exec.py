import logging
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
    Set,
    Tuple,
    Type,
    TypeVar,
)

T = TypeVar("T")
R = TypeVar("R")


class IteratorWrapper(Iterator[T]):
    def __init__(self, iterator: Iterator[T]):
        self.iterator = iterator

    def __next__(self) -> T:
        return next(self.iterator)


@dataclass
class ExceptionContainer(Exception):
    exception: Exception


class ThreadedMappingIteratorWrapper(IteratorWrapper[R]):
    def __init__(self, iterator: Iterator[T], func: Callable[[T], R], n_workers: int):
        super().__init__(iter(ThreadedMappingIterable(iterator, func, n_workers)))

    def __next__(self) -> R:
        elem = super().__next__()
        if isinstance(elem, ExceptionContainer):
            raise elem.exception
        return elem


class ThreadedMappingIterable(Iterable[R]):
    _MAX_QUEUE_SIZE = 32

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
                try:
                    while (
                        not iterator_exhausted
                        and executor._work_queue.qsize()
                        < ThreadedMappingIterable._MAX_QUEUE_SIZE
                        and n_iterated_elems - n_yields
                        < ThreadedMappingIterable._MAX_QUEUE_SIZE
                    ):
                        try:
                            elem = next(self.iterator)
                            n_iterated_elems += 1
                            futures.put(executor.submit(self.func, elem))
                        except StopIteration:
                            iterator_exhausted = True
                    while True:
                        if n_yields < n_iterated_elems:
                            n_yields += 1
                            yield futures.get().result()
                        if iterator_exhausted and n_iterated_elems == n_yields:
                            return
                        if (
                            not iterator_exhausted
                            and executor._work_queue.qsize()
                            < ThreadedMappingIterable._MAX_QUEUE_SIZE // 2
                        ):
                            break
                except Exception as e:
                    yield ExceptionContainer(e)


class ThreadedFlatteningIteratorWrapper(ThreadedMappingIteratorWrapper[T]):
    _SKIP = []
    _BUFFER_SIZE = 32
    _INIT_RETRY_BACKFOFF = 0.0005

    class IteratorIteratorNextsShuffler(Iterator[Callable[[], T]]):
        def __init__(self, iterator_iterator: Iterator[Iterator[T]]):
            self.iterator_iterator = iterator_iterator
            self.iterator_iterator_exhausted = False
            self.iterators_pool: Set[Iterator[T]] = set()
            self.iterators_being_iterated: Set[Iterator[T]] = set()

        def __next__(self):
            backoff = ThreadedFlatteningIteratorWrapper._INIT_RETRY_BACKFOFF
            while True:
                while (
                    not self.iterator_iterator_exhausted
                    and len(self.iterators_pool)
                    < ThreadedFlatteningIteratorWrapper._BUFFER_SIZE
                ):
                    try:
                        elem = next(self.iterator_iterator)
                        if not isinstance(elem, Iterator):
                            raise TypeError(
                                f"Elements to be flattened have to be, but got '{elem}' of type{type(elem)}"
                            )
                        self.iterators_pool.add(elem)
                    except StopIteration:
                        self.iterator_iterator_exhausted = True

                try:
                    next_iterator_elem = self.iterators_pool.pop()
                    self.iterators_being_iterated.add(next_iterator_elem)
                    backoff = ThreadedFlatteningIteratorWrapper._INIT_RETRY_BACKFOFF
                except KeyError:  # KeyError: 'pop from an empty set'
                    if (
                        self.iterator_iterator_exhausted
                        and len(self.iterators_being_iterated) == 0
                    ):
                        raise StopIteration()
                    time.sleep(backoff)
                    backoff *= 2
                    continue

                def f():
                    exhausted = False
                    to_be_raised: Optional[Exception] = None
                    try:
                        elem = next(next_iterator_elem)
                    except StopIteration:
                        exhausted = True
                    except Exception as e:
                        to_be_raised = e
                    self.iterators_being_iterated.remove(next_iterator_elem)
                    if exhausted:
                        return ThreadedFlatteningIteratorWrapper._SKIP
                    else:
                        self.iterators_pool.add(next_iterator_elem)
                    if to_be_raised is not None:
                        raise to_be_raised
                    return elem

                return f

    def __init__(self, iterator: Iterator[Iterator[T]], n_workers: int):
        super().__init__(
            ThreadedFlatteningIteratorWrapper.IteratorIteratorNextsShuffler(iterator),
            func=lambda f: f(),
            n_workers=n_workers,
        )

    def __next__(self) -> T:
        while True:
            elem = super().__next__()
            if elem != ThreadedFlatteningIteratorWrapper._SKIP:
                return elem


class FlatteningIteratorWrapper(IteratorWrapper[R]):
    def __init__(self, iterator: Iterator[Iterator[R]]) -> None:
        super().__init__(iterator)
        self.current_iterator_elem = iter([])

    @staticmethod
    def _sanitize_input(expected_iterator_elem):
        if not isinstance(expected_iterator_elem, Iterator):
            raise TypeError(
                f"Flattened elements must be iterators, but got {type(expected_iterator_elem)}"
            )
        return expected_iterator_elem

    def __next__(self) -> R:
        try:
            return next(
                FlatteningIteratorWrapper._sanitize_input(self.current_iterator_elem)
            )
        except StopIteration:
            while True:
                self.current_iterator_elem = FlatteningIteratorWrapper._sanitize_input(
                    super().__next__()
                )
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
        self, iterator: Iterator[T], classes: Tuple[Type[Exception]], ignore: bool
    ) -> None:
        super().__init__(iterator)
        self.classes = classes
        self.ignore = ignore

    def __next__(self) -> T:
        try:
            return next(self.iterator)
        except StopIteration:
            raise
        except self.classes as e:
            if self.ignore:
                return next(self)  # TODO fix recursion issue
            else:
                return e
