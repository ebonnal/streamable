import itertools
import logging
import multiprocessing
import pickle
import time
import timeit
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
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


class PipeDefinitionError(Exception):
    pass


class Pipe(Iterator[T]):
    """
    Pipe class that represents a data processing pipeline.

    Args:
        source (Union[Iterable[T], Iterator[T]]): The iterator or iterable containing elements for the pipeline.

    Attributes:
        iterator (Iterator[T]): The iterator containing elements for the pipeline.
    """

    def __init__(self, source: Union[Iterable[T], Iterator[T]] = []) -> None:  # timeout
        self.iterator: Iterator[T] = iter(source)
        self._exit_asked: Optional[Event] = None

    def __next__(self) -> T:
        return next(self.iterator)

    def __iter__(self) -> T:
        return self

    def __add__(self, other: "Pipe[T]") -> "Pipe[T]":
        return self.chain(other)

    def __enter__(self) -> "Pipe[T]":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        if self._exit_asked is not None:
            self._exit_asked.set()
        if isinstance(self.iterator, Pipe):
            return self.iterator.__exit__(exc_type, exc_value, traceback)
        return False

    THREAD_WORKER_TYPE = "thread"
    PROCESS_WORKER_TYPE = "process"

    SUPPORTED_WORKER_TYPES = [THREAD_WORKER_TYPE, PROCESS_WORKER_TYPE]

    _MAX_NUM_WAITING_ELEMS_PER_THREAD = 16

    _MANAGER: Optional[multiprocessing.Manager] = None

    def _map_or_do(
        self,
        func: Callable[[T], R],
        n_workers: Optional[int],
        worker_type: str,
        sidify: bool,
    ) -> "Pipe[Union[R, T]]":
        if worker_type not in self.SUPPORTED_WORKER_TYPES:
            raise ValueError(
                f"'{worker_type}' worker_type is not supported, must be one of: {self.SUPPORTED_WORKER_TYPES}"
            )

        if n_workers is None or n_workers <= 0:
            if sidify:
                func = util.sidify(func)
            return Pipe[T](map(func, self))

        if worker_type == Pipe.PROCESS_WORKER_TYPE:
            pickle.dumps(func)
            if Pipe._MANAGER is None:
                Pipe._MANAGER = multiprocessing.Manager()
            self._exit_asked = Pipe._MANAGER.Event()
            queue_class: Type[Queue] = Pipe._MANAGER.Queue
            executor_class: Type[Executor] = ProcessPoolExecutor
        else:
            self._exit_asked = multiprocessing.Event()
            queue_class: Type[Queue] = Queue
            executor_class: Type[Executor] = ThreadPoolExecutor

        return _RasingPipe[T](
            iter(
                _multi_yielding_iterable(
                    func,
                    self,
                    executor_class=executor_class,
                    queue_class=queue_class,
                    n_workers=n_workers,
                    max_queue_size=self._MAX_NUM_WAITING_ELEMS_PER_THREAD * n_workers,
                    sidify=sidify,
                    exit_asked=self._exit_asked,
                )
            )
        )

    def map(
        self,
        func: Callable[[T], R],
        n_workers: Optional[int] = None,
        worker_type: str = THREAD_WORKER_TYPE,
    ) -> "Pipe[R]":
        """
        Apply a function to each element of the Pipe, creating a new Pipe with the mapped elements.

        Args:
            func (Callable[[T], R]): The function to be applied to each element.
            n_workers (int, optional): The number of threads (or processes is worker_type='process') for concurrent func execution (default is 0, meaning single-threaded).
            worker_type (str, optional): Must be Pipe.THREAD_WORKER_TYPE (default) or Pipe.PROCESS_WORKER_TYPE.
        Returns:
            Pipe[R]: A new Pipe instance with elements resulting from applying the function to each element.
        """
        return self._map_or_do(func, n_workers, worker_type, sidify=False)

    def do(
        self,
        func: Callable[[T], Any],
        n_workers: Optional[int] = None,
        worker_type: str = THREAD_WORKER_TYPE,
    ) -> "Pipe[T]":
        """
        Run the func as side effect: the resulting Pipe forwards the upstream elements after func execution's end.

        Args:
            func (Callable[[T], R]): The function to be applied to each element.
            n_workers (int, optional): The number of threads (or processes is worker_type='process') for concurrent func execution (default is 0, meaning single-threaded).
            worker_type (str, optional): Must be Pipe.THREAD_WORKER_TYPE (default) or Pipe.PROCESS_WORKER_TYPE.
        Returns:
            Pipe[T]: A new Pipe instance with elements resulting from applying the function to each element.
        """
        return self._map_or_do(func, n_workers, worker_type, sidify=True)

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

    def batch(self, size: int = 100, secs: float = float("inf")) -> "Pipe[List[T]]":
        """
        Batch elements of the Pipe into lists of a specified size or within a specified time window.

        Args:
            size (int, optional): The maximum number of elements per batch (default is 100).
            secs (float, optional): The maximum time window for batching elements (default is infinity).

        Returns:
            Pipe[List[T]]: A new Pipe instance with lists containing batches of elements.
        """
        return _BatchingPipe[T](self, size, secs)

    def slow(self, freq: float) -> "Pipe[T]":
        """
        Slow down the iteration of elements in the Pipe, creating a new Pipe with a specified frequency.

        Args:
            freq (float): The frequency (in milliseconds) at which elements are iterated.

        Returns:
            Pipe[T]: A new Pipe instance with elements iterated at the specified frequency.
        """
        return Pipe[T](_SlowingPipe(self, freq))

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
            List[T]: A list containing the elements of the Pipe.
        """
        return [elem for i, elem in enumerate(self) if i < n_samples]

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
            .log("ultimate elements")
            .filter(lambda elem: isinstance(elem, Exception))
            .map(repr)
            .collect(n_samples=n_error_samples)
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
        except PipeDefinitionError:
            raise
        except self.classes as e:
            if self.ignore:
                return next(self)
            else:
                return e


class _LoggingPipe(Pipe[T]):
    def __init__(self, iterator: Iterator[T], what: str) -> None:
        super().__init__(iterator)
        logging.getLogger().setLevel(logging.INFO)
        self.what = what
        self.yields_count = 0
        self.errors_count = 0
        self.last_log_at_yields_count = 0
        self.start_time = time.time()

    def _log(self) -> None:
        logging.info(
            "%s `%s` have been yielded in elapsed time '%s', with %s errors produced.",
            self.yields_count,
            self.what,
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
            if self.yields_count % 2:
                self._log()
            raise

        self.yields_count += 1
        if isinstance(elem, Exception):
            self.errors_count += 1

        if self.yields_count >= 2 * self.last_log_at_yields_count:
            self._log()
            self.last_log_at_yields_count = self.yields_count + self.errors_count

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


E = TypeVar("E", bound=Exception)


class _CatchedError(Generic[E]):
    def __init__(self, exception: E) -> None:
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


class _BatchingPipe(Pipe[List[T]]):
    def __init__(self, iterator: Iterator[T], size: int, secs: float) -> None:
        super().__init__(iterator)
        self.size = size
        self.secs = secs

    def __next__(self) -> List[T]:
        start_time = time.time()
        batch = None
        try:
            batch = [super().__next__()]
            while len(batch) < self.size and (time.time() - start_time) < self.secs:
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
        iterators: List[Iterator[T]], queue: Queue
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
            backoff_secs = 0.005
            while not queue.empty() or not all((future.done() for future in futures)):
                try:
                    yield queue.get(timeout=backoff_secs)
                    backoff_secs = 0.005
                except Empty:
                    backoff_secs *= 2


def _mapper(
    func: Callable[[Any], Any],
    input_queue: Queue,
    output_queue: Queue,
    sidify: bool,
    exit_asked: Event,
    sentinel: Any,
) -> None:
    while not exit_asked.is_set():
        try:
            elem = input_queue.get(timeout=0.1)
            if elem == sentinel:
                input_queue.put(sentinel)
                break
        except Empty:
            continue
        try:
            res = func(elem)
            to_output = elem if sidify else res
        except Exception as e:
            to_output = _CatchedError(e)
        while not exit_asked.is_set():
            try:
                output_queue.put(to_output, timeout=0.1)
                break
            except Empty:
                pass


def _feeder(
    input_queue: Queue, iterator: Iterator[T], exit_asked: Event, sentinel: Any
):
    while not exit_asked.is_set():
        try:
            elem = next(iterator)
        except StopIteration:
            while not exit_asked.is_set():
                try:
                    input_queue.put(sentinel, timeout=0.1)
                    break
                except Full:
                    pass
            break
        except Exception as e:
            elem = _CatchedError(e)
        while not exit_asked.is_set():
            try:
                input_queue.put(elem, timeout=0.1)
                break
            except Full:
                pass


def _multi_yielding_iterable(
    func: Callable[[T], R],
    iterator: Iterator[T],
    executor_class: Type[Executor],
    queue_class: Type[Queue],
    n_workers: int,
    max_queue_size: int,
    sidify: bool,
    exit_asked: Event,
) -> Iterator[Union[T, R]]:
    input_queue: Queue = queue_class(maxsize=max_queue_size)
    output_queue: Queue = queue_class(maxsize=max_queue_size)

    with executor_class(max_workers=n_workers) as executor:
        sentinel = f"__kioss__{time.time()}"
        futures = [
            executor.submit(
                _mapper, func, input_queue, output_queue, sidify, exit_asked, sentinel
            )
            for _ in range(n_workers)
        ]
        with ThreadPoolExecutor(max_workers=1) as executor:
            futures.append(
                executor.submit(_feeder, input_queue, iterator, exit_asked, sentinel)
            )
            backoff_secs = 0.005
            while not exit_asked.is_set() and (
                not all((future.done() for future in futures))
                or not output_queue.empty()
            ):
                try:
                    yield output_queue.get(timeout=backoff_secs)
                    backoff_secs = 0.005
                except Empty:
                    backoff_secs *= 2
    # yield worker errors not linked to an element processing
    for future in futures:
        try:
            future.result()
        except Exception as e:
            yield _CatchedError(e)
