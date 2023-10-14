from dataclasses import dataclass
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

    def __init__(self, source: Union[Iterable[T], Iterator[T]] = []) -> None:
        self.iterator: Iterator[T] = iter(source)
        self._exit_asked: Optional[Event] = None
        self._upstream: List[Pipe[T]] = []
        if isinstance(source, Pipe):
            self._upstream.append(source)

    def _with_upstream(self, *pipes: "Pipe[T]") -> "Pipe[T]":
        self._upstream.extend(pipes)
        return self

    def __next__(self) -> T:
        return next(self.iterator)

    def __iter__(self) -> Iterator[T]:
        return self

    def __add__(self, other: "Pipe[T]") -> "Pipe[T]":
        return self.chain(other)

    def __enter__(self) -> "Pipe[T]":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        if self._exit_asked is not None:
            self._exit_asked.set()
        for upstream_pipe in self._upstream:
            upstream_pipe.__exit__(exc_type, exc_value, traceback)
        return False

    THREAD_WORKER_TYPE = "thread"
    PROCESS_WORKER_TYPE = "process"

    SUPPORTED_WORKER_TYPES = [THREAD_WORKER_TYPE, PROCESS_WORKER_TYPE]

    _MAX_NUM_WAITING_ELEMS_PER_WORKER = 8

    _MANAGER: Optional[multiprocessing.Manager] = None

    def _map(
        self: "Pipe[Union[Iterator[T], T]]",
        func: Callable[[T], R],
        n_workers: Optional[int],
        worker_type: str,
        sidify: bool,
        flatten: bool,
    ) -> "Pipe[Union[R, T]]":
        if flatten and (func is not None or sidify is not None):
            raise ValueError(
                "_map cannot be called with flatten=True and non None func or sidify"
            )
        if sidify and flatten is not None:
            raise ValueError(
                "_map cannot be called with sidify=True and non None flatten"
            )
        if worker_type not in Pipe.SUPPORTED_WORKER_TYPES:
            raise ValueError(
                f"'{worker_type}' worker_type is not supported, must be one of: {Pipe.SUPPORTED_WORKER_TYPES}"
            )
        if n_workers is None or n_workers <= 0:
            if flatten:
                return _FlatteningPipe[R](self)
            else:
                func = util.map_exception(
                    func, source=StopIteration, target=RuntimeError
                )
                if sidify:
                    func = util.sidify(func)
                return Pipe[T](map(func, self))._with_upstream(self)

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
                    max_queue_size=self._MAX_NUM_WAITING_ELEMS_PER_WORKER * n_workers,
                    sidify=sidify,
                    flatten=flatten,
                    exit_asked=self._exit_asked,
                )
            )
        )._with_upstream(self)

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
            worker_type (str, optional): Must be Pipe.THREAD_WORKER_TYPE or Pipe.PROCESS_WORKER_TYPE (default is THREAD_WORKER_TYPE).
        Returns:
            Pipe[R]: A new Pipe instance with elements resulting from applying the function to each element.
        """
        return self._map(func, n_workers, worker_type, sidify=False, flatten=None)

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
            n_workers (int, optional): The number of threads (or processes if worker_type='process') for concurrent func execution (default is None, meaning single-threaded).
            worker_type (str, optional): Must be Pipe.THREAD_WORKER_TYPE or Pipe.PROCESS_WORKER_TYPE (default is THREAD_WORKER_TYPE).
        Returns:
            Pipe[T]: A new Pipe instance with elements resulting from applying the function to each element.
        """
        return self._map(func, n_workers, worker_type, sidify=True, flatten=None)

    def flatten(
        self: "Pipe[Iterator[R]]",
        n_workers: Optional[int] = None,
        worker_type: str = THREAD_WORKER_TYPE,
    ) -> "Pipe[R]":
        """
        Flatten the elements of the Pipe, which are assumed to be iterators, creating a new Pipe with individual elements.

        Returns:
            Pipe[R]: A new Pipe instance with individual elements obtained by flattening the original elements.
            n_workers (int, optional): The number of threads (or processes if worker_type='process') for concurrent flattening execution (default is None, meaning single-threaded).
            worker_type (str, optional): Must be Pipe.THREAD_WORKER_TYPE or Pipe.PROCESS_WORKER_TYPE (default is THREAD_WORKER_TYPE).
        """
        return self._map(None, n_workers, worker_type, sidify=None, flatten=True)

    def chain(self, *others: "Pipe[T]") -> "Pipe[T]":
        """
        Create a new Pipe by chaining the elements of this Pipe with the elements from other Pipes. The elements of a given Pipe are yielded after its predecessor Pipe is exhausted.

        Args:
            *others (Pipe[T]): One or more additional Pipe instances to chain with this Pipe.

        Returns:
            Pipe[T]: A new Pipe instance with elements from this Pipe followed by elements from other Pipes.
        """
        return Pipe[T](itertools.chain(self, *others))._with_upstream(self, *others)

    def filter(self, predicate: Callable[[T], bool]) -> "Pipe[T]":
        """
        Filter the elements of the Pipe based on the given predicate, creating a new Pipe with filtered elements.

        Args:
            predicate (Callable[[T], bool]): The function that determines whether an element should be included.

        Returns:
            Pipe[T]: A new Pipe instance with elements that satisfy the predicate.
        """
        return Pipe[T](filter(predicate, self))._with_upstream(self)

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
        except PipeDefinitionError:
            raise
        except self.classes as e:
            if self.ignore:
                return next(self)
            else:
                return e


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


E = TypeVar("E", bound=Exception)





class _RasingPipe(Pipe[T]):
    class CatchedError(Generic[E]):
        def __init__(self, exception: E) -> None:
            super().__init__()
            self.exception: E = exception

    def __next__(self) -> T:
        next_elem = super().__next__()
        if isinstance(next_elem, _RasingPipe.CatchedError):
            raise next_elem.exception
        else:
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


_uuid = "ad0a24ef-ad56-4597-a43e-d02e1d4fc5da"
_NOTHING = f"__kioss__nothing_{_uuid}"
_SENTINEL = f"__kioss__sentinel_{_uuid}"


def _pull(
    input_queue: Queue,
    exit_asked: Event,
) -> Any:
    elem = _NOTHING
    while not exit_asked.is_set():
        try:
            elem = input_queue.get(timeout=0.1)
            if elem == _SENTINEL:
                input_queue.put(_SENTINEL)
            break
        except Empty:
            continue
    return elem


def _put(
    to_output: Any,
    output_queue: Queue,
    exit_asked: Event,
) -> None:
    while not exit_asked.is_set():
        try:
            output_queue.put(to_output, timeout=0.1)
            break
        except Full:
            pass


def _mapper(
    func: Callable[[Any], Any],
    input_queue: Queue,
    output_queue: Queue,
    sidify: bool,
    exit_asked: Event,
) -> None:
    while not exit_asked.is_set():
        elem = _pull(input_queue, exit_asked)
        if elem == _NOTHING or elem == _SENTINEL:
            break
        if isinstance(elem, _RasingPipe.CatchedError):
            _put(elem, output_queue, exit_asked)
        else:
            try:
                res = func(elem)
                to_output = elem if sidify else res
            except StopIteration as e:
                # raising a stop iteration would completely mess downstream iteration
                remapped_exception = RuntimeError()
                remapped_exception.__cause__ = e
                to_output = _RasingPipe.CatchedError(remapped_exception)
            except Exception as e:
                to_output = _RasingPipe.CatchedError(e)
            _put(to_output, output_queue, exit_asked)


def _flat_mapper(
    input_queue: Queue,
    output_queue: Queue,
    exit_asked: Event,
) -> None:
    while not exit_asked.is_set():
        elem = _pull(input_queue, exit_asked)
        if elem == _NOTHING or elem == _SENTINEL:
            break
        if isinstance(elem, _RasingPipe.CatchedError):
            _put(elem, output_queue, exit_asked)
        else:
            iterator = None
            while not exit_asked.is_set():
                try:
                    if iterator is None:
                        iterator = iter(elem)
                except StopIteration as e:
                    # raising a stop iteration would completely mess downstream iteration
                    remapped_exception = RuntimeError()
                    remapped_exception.__cause__ = e
                    _put(_RasingPipe.CatchedError(remapped_exception), output_queue, exit_asked)
                    break
                except Exception as e:
                    _put(_RasingPipe.CatchedError(e), output_queue, exit_asked)
                    break
                try:
                    to_output = next(iterator)
                except StopIteration as e:
                    break
                except Exception as e:
                    to_output = _RasingPipe.CatchedError(e)
                _put(to_output, output_queue, exit_asked)


def _feeder(input_queue: Queue, pipe: Iterator[T], exit_asked: Event):
    while not exit_asked.is_set():
        try:
            elem = next(pipe)
            if isinstance(elem, Pipe):
                pipe._with_upstream(elem)
        except StopIteration:
            _put(_SENTINEL, input_queue, exit_asked)
            break
        except Exception as e:
            elem = _RasingPipe.CatchedError(e)
        _put(elem, input_queue, exit_asked)


def _multi_yielding_iterable(
    func: Callable[[T], R],
    pipe: Pipe[T],
    executor_class: Type[Executor],
    queue_class: Type[Queue],
    n_workers: int,
    max_queue_size: int,
    sidify: bool,
    flatten: bool,
    exit_asked: Event,
) -> Iterator[Union[T, R]]:
    input_queue: Queue = queue_class(maxsize=max_queue_size)
    output_queue: Queue = queue_class(maxsize=max_queue_size)

    with executor_class(max_workers=n_workers) as executor:
        futures = [
            executor.submit(_flat_mapper, input_queue, output_queue, exit_asked)
            if flatten
            else executor.submit(
                _mapper, func, input_queue, output_queue, sidify, exit_asked
            )
            for _ in range(n_workers)
        ]
        with ThreadPoolExecutor(max_workers=1) as executor:
            futures.append(executor.submit(_feeder, input_queue, pipe, exit_asked))
            while not exit_asked.is_set() and (
                not all((future.done() for future in futures))
                or not output_queue.empty()
            ):
                try:
                    yield output_queue.get(timeout=0.1)
                except Empty:
                    pass
    # yield worker errors not linked to an element processing
    for future in futures:
        try:
            future.result()
        except Exception as e:
            yield _RasingPipe.CatchedError(e)


def f(x):
    return x**2
