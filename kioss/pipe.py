import atexit
import itertools
import logging
import time
import timeit
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import reduce
from queue import Empty, Queue
from typing import Callable, Iterable, Iterator, List, Tuple, Type, TypeVar, Union

import kioss.util as util

T = TypeVar("T")
R = TypeVar("R")


class Pipe(Iterator[T]):
    """
    Pipe class that represents a data processing pipeline.

    Args:
        source (Union[Iterable[T], Iterator[T]]): The iterator or iterable containing elements for the pipeline.

    Attributes:
        iterator (Iterator[T]): The iterator containing elements for the pipeline.
    """

    def __init__(self, source: Union[Iterable[T], Iterator[T]] = []) -> None:
        self.iterator: Iterator[T] = iter(source)

    def __next__(self) -> T:
        return next(self.iterator)

    def __iter__(self) -> T:
        return self

    def __add__(self, other: "Pipe[T]") -> "Pipe[T]":
        return self.chain(other)

    def chain(self, *others: Tuple["Pipe[T]"]) -> "Pipe[T]":
        """
        Create a new Pipe by chaining the elements of this Pipe with the elements from other Pipes. The elements of a given Pipe are yielded after its predecessor Pipe is exhausted.

        Args:
            *others ([Pipe[T]]): One or more additional Pipe instances to chain with this Pipe.

        Returns:
            Pipe[T]: A new Pipe instance with elements from this Pipe followed by elements from other Pipes.
        """
        return Pipe[T](itertools.chain(self, *others))

    def mix(self, *others: "Pipe[T]") -> "Pipe[T]":
        """
        Mix this Pipe with other Pipes using one thread per pipe, returning a new Pipe instance concurrently yielding elements from self and others in any order.

        Args:
            *others ([Pipe[T]]): One or more additional Pipe instances to mix with this Pipe.

        Returns:
            Pipe[T]: A new Pipe instance concurrently yielding elements from self and others in any order.
        """
        return _RasingPipe[R](_ConcurrentlyMergingPipe[T]([self, *others]))

    def map(
        self, func: Callable[[T], R], n_threads: int = 0, sidify: bool = False
    ) -> "Pipe[R]":
        """
        Apply a function to each element of the Pipe, creating a new Pipe with the mapped elements.

        Args:
            func (Callable[[T], R]): The function to be applied to each element.
            n_threads (int, optional): The number of threads for concurrent mapping (default is 0, meaning single-threaded).
            sidify (bool, optional): If True, apply function with side effect wrapping (default is False).

        Returns:
            Pipe[R]: A new Pipe instance with elements resulting from applying the function to each element.
        """
        if sidify:
            func = util.sidify(func)

        if n_threads <= 0:
            return Pipe[R](map(func, self))
        else:
            return _RasingPipe[R](_ConcurrentlyMappingPipe[R](func, self, n_threads))

    def flatten(self: "Pipe[Iterator[R]]") -> "Pipe[R]":
        """
        Flatten the elements of the Pipe, which are assumed to be iterators, creating a new Pipe with individual elements.

        Returns:
            Pipe[R]: A new Pipe instance with individual elements obtained by flattening the original elements.
        """
        return _FlatteningPipe[R](self)

    def filter(self, predicate: Callable[[T], bool]) -> "Pipe[T]":
        """
        Filter the elements of the Pipe based on the given predicate, creating a new Pipe with filtered elements.

        Args:
            predicate (Callable[[T], bool]): The function that determines whether an element should be included.

        Returns:
            Pipe[T]: A new Pipe instance with elements that satisfy the predicate.
        """
        return Pipe[T](filter(predicate, self))

    def batch(
        self, max_size: int = 100, time_window_seconds: float = float("inf")
    ) -> "Pipe[List[T]]":
        """
        Batch elements of the Pipe into lists of a specified size or within a specified time window.

        Args:
            max_size (int, optional): The maximum number of elements per batch (default is 100).
            max_window_seconds (float, optional): The maximum time window for batching elements (default is infinity).

        Returns:
            Pipe[List[T]]: A new Pipe instance with lists containing batches of elements.
        """
        return _BatchingPipe[T](self, max_size, time_window_seconds)

    def slow(self, freq: float) -> "Pipe[T]":
        """
        Slow down the iteration of elements in the Pipe, creating a new Pipe with a specified frequency.

        Args:
            freq (float): The frequency (in milliseconds) at which elements are iterated.

        Returns:
            Pipe[T]: A new Pipe instance with elements iterated at the specified frequency.
        """
        return Pipe[T](_SlowingIterator(self, freq))

    def head(self, n: int) -> "Pipe[T]":
        """
        Limit the number of elements in the Pipe, creating a new Pipe with the first `n` elements.

        Args:
            n (int): The number of elements to include in the new Pipe.

        Returns:
            Pipe[T]: A new Pipe instance with the first `n` elements.
        """
        return Pipe[T](itertools.islice(self, n))

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

    def log(self, objects_description: str = "elements") -> "Pipe[T]":
        """
        Log the elements of the Pipe as they are iterated.

        Returns:
            Pipe[T]: A new Pipe instance with logging capability.
        """
        return _LoggingPipe[T](self, objects_description)

    def reduce(self, func: Callable[[R, T], R], initial: R) -> R:
        """
        Reduce the elements of the Pipe using a binary function, starting from the given initial value.

        Args:
            func (Callable[[R, T], R]): The binary function used for reduction.
            initial (R): The initial value for the reduction.

        Returns:
            R: The result of the reduction.
        """
        return reduce(func, self, initial)

    def collect(self, limit: int = float("inf")) -> List[T]:
        """
        Convert the elements of the Pipe into a list. The entire pipe will be iterated, even after reaching the optional limit.

        Args:
            limit (int, optional): The maximum number of elements to include in the list (default is infinity).

        Returns:
            List[T]: A list containing the elements of the Pipe.
        """
        return [elem for i, elem in enumerate(self) if i < limit]

    def time(self) -> float:
        """
        Measure the time taken to iterate through all the elements in the Pipe.

        Returns:
            float: The time taken in seconds.
        """

        def iterate():
            for _ in self:
                pass

        return timeit.timeit(iterate, number=1)

    def superintend(self, n_error_samples: int = 8) -> None:
        """
        Superintend the Pipe: iterate over the pipe until it is exhausted and raise a RuntimeError if any exceptions occur during iteration.

        Args:
            n_error_samples (int, optional): The maximum number of error samples to include in the RuntimeError message (default is 8).
        Raises:
            RuntimeError: If any exception is catched during iteration.
        """
        if errors := (
            self.catch(Exception, ignore=False)
            .log(objects_description="ultimate elements")
            .filter(lambda elem: isinstance(elem, Exception))
            .map(repr)
            .collect(limit=n_error_samples)
        ):
            raise RuntimeError(errors)


class _FlatteningPipe(Pipe[R]):
    def __init__(self, iterator: Iterator[Iterator[R]]) -> None:
        super().__init__(iterator)
        self.current_iterator_elem = iter(super().__next__())

    def __next__(self) -> R:
        try:
            return next(self.current_iterator_elem)
        except StopIteration:
            self.current_iterator_elem = iter(super().__next__())
            return next(self)


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


class _LoggingPipe(Pipe[T]):
    def __init__(self, iterator: Iterator[T], objects_description: str) -> None:
        super().__init__(iterator)
        logging.getLogger().setLevel(logging.INFO)
        self.objects_description = objects_description
        self.yields_count = 0
        self.errors_count = 0
        self.last_log_at_yields_count = 0
        self.start_time = time.time()

    def _log(self) -> None:
        logging.info(
            "%s `%s` have been yielded in elapsed time '%s', with %s errors produced.",
            self.yields_count,
            self.objects_description,
            str(
                datetime.fromtimestamp(time.time())
                - datetime.fromtimestamp(self.start_time)
            ),
            self.errors_count,
        )

    def __next__(self) -> T:
        try:
            elem = super().__next__()
        except StopIteration:
            self._log()
            raise

        self.yields_count += 1
        if isinstance(elem, Exception):
            self.errors_count += 1

        if self.yields_count + self.errors_count >= 2 * self.last_log_at_yields_count:
            self._log()
            self.last_log_at_yields_count = self.yields_count + self.errors_count

        return elem


class _SlowingIterator(Pipe[T]):
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
    def __init__(
        self, iterator: Iterator[T], max_size: int, max_window_seconds: float
    ) -> None:
        super().__init__(iterator)
        self.max_size = max_size
        self.max_window_seconds = max_window_seconds

    def __next__(self) -> List[T]:
        start_time = time.time()
        batch = [super().__next__()]
        try:
            while (
                len(batch) < self.max_size
                and (time.time() - start_time) < self.max_window_seconds
            ):
                batch.append(super().__next__())
            return batch
        except StopIteration:
            if batch:
                return batch
            else:
                raise


class _ConcurrentlyMergingPipe(Pipe[T]):
    MAX_NUM_WAITING_ELEMS_PER_THREAD = 16

    def __init__(self, iterators: List[Iterator[T]]) -> None:
        super().__init__(
            self._concurrently_merging_iterable(
                iterators,
                Queue(self.MAX_NUM_WAITING_ELEMS_PER_THREAD * len(iterators)),
            )
        )

    @staticmethod
    def _concurrently_merging_iterable(
        iterators: List[Iterator[T]], queue: Queue[T]
    ) -> Iterator[T]:
        with ThreadPoolExecutor(max_workers=len(iterators)) as executor:

            def pull_job(iterator: Iterator[T]):
                while True:
                    try:
                        queue.put(next(iterator))
                    except StopIteration:
                        break
                    except Exception as e:
                        queue.put(_CatchedError(e))

            futures = [executor.submit(pull_job, iterator) for iterator in iterators]
            while not queue.empty() or not all((future.done() for future in futures)):
                try:
                    yield queue.get(timeout=0.01)
                except Empty:
                    pass


class _ConcurrentlyMappingPipe(Pipe[R]):
    MAX_NUM_WAITING_ELEMS_PER_THREAD = 16

    def __init__(
        self, func: Callable[[T], R], iterator: Iterator[T], n_threads: int
    ) -> None:
        super().__init__(
            iter(
                self._concurrently_mapping_iterable(
                    func,
                    iterator,
                    n_threads=n_threads,
                    max_queue_size=self.MAX_NUM_WAITING_ELEMS_PER_THREAD * n_threads,
                )
            )
        )

    @staticmethod
    def _concurrently_mapping_iterable(
        func: Callable[[T], R], iterator: Iterator[T], n_threads, max_queue_size: int
    ) -> Iterator[T]:
        input_queue: Queue[T] = Queue(maxsize=max_queue_size)
        output_queue: Queue[R] = Queue(maxsize=max_queue_size)

        with ThreadPoolExecutor(max_workers=n_threads + 1) as executor:
            input_feeder_future = executor.submit(
                lambda: util.iterate(map(input_queue.put, iterator))
            )

            def mapper_job():
                while not input_feeder_future.done() or not input_queue.empty():
                    try:
                        output_queue.put(func(input_queue.get(timeout=0.01)))
                    except Empty:
                        pass
                    except Exception as e:
                        output_queue.put(_CatchedError(e))

            futures = [executor.submit(mapper_job) for _ in range(n_threads)]
            while (
                not all((future.done() for future in futures))
                or not output_queue.empty()
            ):
                try:
                    yield output_queue.get(timeout=0.01)
                except Empty:
                    pass


class _CatchedError:
    def __init__(self, exception: Exception) -> None:
        super().__init__()
        self.exception: E = exception


class _RasingPipe(Pipe[T]):
    def __init__(self, iterator: Iterator[T]) -> None:
        super().__init__(iterator)

    def __next__(self) -> T:
        next_elem = super().__next__()
        if isinstance(next_elem, _CatchedError):
            raise next_elem.exception
        else:
            return next_elem
