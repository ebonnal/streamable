from dataclasses import dataclass
import itertools
import logging
import multiprocessing
import pickle
import time
import timeit
from concurrent.futures import Executor, Future, ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime
from multiprocessing.synchronize import Event
from queue import Empty, Full, Queue
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import kioss.util as util

T = TypeVar("T")
R = TypeVar("R")


class Pipe(Iterator[T]):
    """
    Args:
        source (Union[Iterable[T], Iterator[T]]): The iterator or iterable containing elements for the pipeline.

    Attributes:
        iterator (Iterator[T]): The iterator containing elements for the pipeline.
    """

    def __init__(self, source: Union[Iterable[T], Iterator[T]]) -> None:
        self.iterator = iter(source)

    def __next__(self) -> T:
        return next(self.iterator)

    def __iter__(self) -> Iterator[T]:
        return self

    def __add__(self, other: "Pipe[T]") -> "Pipe[T]":
        return self.chain(other)


    @staticmethod
    def sanitize_n_threads(n_threads: int):
        if not isinstance(n_threads, int):
            raise TypeError(f"n_threads should be an int but got '{n_threads}' of type {type(n_threads)}.")
        if n_threads < 1:
            raise ValueError(f"n_threads should be greater or equal to 1, but got {n_threads}.")

    def map(
        self,
        func: Callable[[T], R],
        n_threads: int = 1,
    ) -> "Pipe[R]":
        """
        Apply a function to each element of the Pipe, creating a new Pipe with the mapped elements.

        Args:
            func (Callable[[T], R]): The function to be applied to each element.
            n_threads (int): The number of threads for concurrent execution (default is 1, meaning only the main thread is used).
        Returns:
            Pipe[R]: A new Pipe instance with elements resulting from applying the function to each element.
        """
        Pipe.sanitize_n_threads(n_threads)
        func = util.map_exception(func, source=StopIteration, target=RuntimeError)
        if n_threads == 1:
            return Pipe[T](map(func, self))
        else:
            return _ThreadedMapperPipe(self, func, n_workers = n_threads - 1)

    def do(
        self,
        func: Callable[[T], Any],
        n_threads: int = 1,
    ) -> "Pipe[T]":
        """
        Run the func as side effect: the resulting Pipe forwards the upstream elements after func execution's end.

        Args:
            func (Callable[[T], R]): The function to be applied to each element.
            n_threads (int): The number of threads for concurrent execution (default is 1, meaning only the main thread is used).
        Returns:
            Pipe[T]: A new Pipe instance with elements resulting from applying the function to each element.
        """
        return self.map(util.sidify(func), n_threads)

    def flatten(
        self: "Pipe[Iterator[R]]",
        n_threads: int = 1,
    ) -> "Pipe[R]":
        """
        Flatten the elements of the Pipe, which are assumed to be iterators, creating a new Pipe with individual elements.

        Returns:
            Pipe[R]: A new Pipe instance with individual elements obtained by flattening the original elements.
            n_threads (int): The number of threads for concurrent execution (default is 1, meaning only the main thread is used).
        """
        Pipe.sanitize_n_threads(n_threads)
        return _FlatteningPipe[R](self)

    def chain(self, *others: "Pipe[T]") -> "Pipe[T]":
        """
        Create a new Pipe by chaining the elements of this Pipe with the elements from other Pipes. The elements of a given Pipe are yielded after its predecessor Pipe is exhausted.

        Args:
            *others (Pipe[T]): One or more additional Pipe instances to chain with this Pipe.

        Returns:
            Pipe[T]: A new Pipe instance with elements from this Pipe followed by elements from other Pipes.
        """
        return Pipe[T](itertools.chain(self, *others))

    def filter(self, predicate: Callable[[T], bool]) -> "Pipe[T]":
        """
        Filter the elements of the Pipe based on the given predicate, creating a new Pipe with filtered elements.

        Args:
            predicate (Callable[[T], bool]): The function that determines whether an element should be included.

        Returns:
            Pipe[T]: A new Pipe instance with elements that satisfy the predicate.
        """
        return Pipe[T](filter(predicate, self))

    def batch(self, size: int = 100, period: float = float("inf")) -> "Pipe[List[T]]":
        """
        Batch elements of the Pipe into lists of a specified size or within a specified time window.

        Args:
            size (int, optional): The maximum number of elements per batch (default is 100).
            period (float, optional): The maximum number of seconds to wait before yielding a batch (default is infinity).

        Returns:
            Pipe[List[T]]: A new Pipe instance with lists containing batches of elements.
        """
        return _BatchingPipe[T](self, size, period)

    def slow(self, freq: float) -> "Pipe[T]":
        """
        Slow down the iteration of elements in the Pipe, creating a new Pipe with a specified frequency.

        Args:
            freq (float): The frequency (in milliseconds) at which elements are iterated.

        Returns:
            Pipe[T]: A new Pipe instance with elements iterated at the specified frequency.
        """
        return _SlowingPipe[T](self, freq)

    def catch(self, *classes: Type[Exception], ignore=False) -> "Pipe[T]":
        """
        Any error whose class is exception_class or a subclass of it will be catched and yielded.

        Args:
            exception_class (Type[Exception]): The class of exceptions to catch
            ingore (bool): If True then the encountered exception_class errors will be skipped.

        Returns:
            Pipe[T]: A new Pipe instance with error handling capability.
        """
        return _CatchingPipe[T](self, classes, ignore)

    def log(self, what: str = "elements") -> "Pipe[T]":
        """
        Log the elements of the Pipe as they are iterated.

        Args:
            what (str): name the objects yielded by the pipe for clearer logs, must be a plural descriptor.

        Returns:
            Pipe[T]: A new Pipe instance with logging capability.
        """
        return _LoggingPipe[T](self, what)

    def collect(self, n_samples: int = float("inf")) -> List[T]:
        """
        Convert the elements of the Pipe into a list. The entire pipe will be iterated, but only n_samples elements will be saved in the returned list.

        Args:
            n_samples (int, optional): The maximum number of elements to collect in the list (default is infinity).

        Returns:
            List[T]: A list containing the elements of the Pipe truncate to the first `n_samples` ones.
        """
        return [elem for i, elem in enumerate(self) if i < n_samples]

    def superintend(self, n_samples: int = 0, n_error_samples: int = 8) -> List[T]:
        """
        Superintend the Pipe: iterate over the pipe until it is exhausted and raise a RuntimeError if any exceptions occur during iteration.

        Args:
            n_samples (int, optional): The maximum number of elements to collect in the list (default is infinity).
            n_error_samples (int, optional): The maximum number of error samples to log (default is 8).
        Returns:
            List[T]: A list containing the elements of the Pipe truncate to the first `n_samples` ones.
        Raises:
            RuntimeError: If any exception is catched during iteration.
        """
        if not isinstance(self, _LoggingPipe):
            pipe = self.log("output elements")
        else:
            pipe = self
        error_samples: List[Exception] = []
        samples = (
            pipe.catch(Exception, ignore=False)
            .do(
                lambda elem: error_samples.append(elem)
                if isinstance(elem, Exception) and len(error_samples) < n_error_samples
                else None
            )
            .filter(lambda elem: not isinstance(elem, Exception))
            .collect(n_samples=n_samples)
        )
        if len(error_samples):
            logging.error(
                "first %s error samples: %s\nWill now raise the first of them:",
                n_error_samples,
                list(map(repr, error_samples)),
            )
            raise error_samples[0]

        return samples


class _CatchingPipe(Pipe[T]):
    def __init__(
        self, iterator: Iterator[T], classes: Tuple[Type[Exception]], ignore: bool
    ) -> None:
        super().__init__(iterator)
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

class _ThreadedMapperPipe(Pipe[R]):
    _MAX_QUEUE_SIZE = 16
    def __init__(self, iterator: Iterator[T], func: Callable[[T], R], n_workers: int):
        self.n_workers = n_workers
        self.iterator = iterator
        self.func = func

    def __iter__(self):
        futures: "Queue[Future]" = Queue()
        iterator_exhausted = False
        n_yields = 0
        n_iterated_elems = 0
        with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
            while True:
                while not iterator_exhausted and executor._work_queue.qsize() < _ThreadedMapperPipe._MAX_QUEUE_SIZE:
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
                    if not iterator_exhausted and executor._work_queue.qsize() < _ThreadedMapperPipe._MAX_QUEUE_SIZE//2:
                        break
                

class _FlatteningPipe(Pipe[R]):
    def __init__(self, iterator: Iterator[Iterator[R]]) -> None:
        super().__init__(iterator)
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

class _LoggingPipe(Pipe[T]):
    def __init__(self, iterator: Iterator[T], what: str) -> None:
        super().__init__(iterator)
        logging.getLogger().setLevel(logging.INFO)
        self.what = what
        self.yields_count = 0
        self.errors_count = 0
        self.last_log_at_yields_count = None
        self.start_time = time.time()
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


class _SlowingPipe(Pipe[T]):
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


class _BatchingPipe(Pipe[List[T]]):
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
