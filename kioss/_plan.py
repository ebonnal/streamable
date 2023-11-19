import itertools
import logging
from abc import ABC, abstractmethod
from typing import (
    Any,
    Callable,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

from kioss import _exec, _concurrent_exec, _util

T = TypeVar("T")
R = TypeVar("R")


class APipe(Iterable[T], ABC):
    def __init__(self, upstream: "Optional[APipe[T]]" = None):
        self.upstream = upstream

    @abstractmethod
    def __iter__(self) -> Iterator[T]:
        raise NotImplemented()

    def __add__(self, other: "APipe[T]") -> "APipe[T]":
        return self.chain(other)

    @staticmethod
    def sanitize_n_threads(n_threads: int):
        if not isinstance(n_threads, int):
            raise TypeError(
                f"n_threads should be an int but got '{n_threads}' of type {type(n_threads)}."
            )
        if n_threads < 1:
            raise ValueError(
                f"n_threads should be greater or equal to 1, but got {n_threads}."
            )

    def map(
        self,
        func: Callable[[T], R],
        n_threads: int = 1,
    ) -> "APipe[R]":
        """
        Apply a function to each element of the Pipe, creating a new Pipe with the mapped elements.

        Args:
            func (Callable[[T], R]): The function to be applied to each element.
            n_threads (int): The number of threads for concurrent execution (default is 1, meaning only the main thread is used).
        Returns:
            Pipe[R]: A new Pipe instance with elements resulting from applying the function to each element.
        """
        APipe.sanitize_n_threads(n_threads)
        func = _util.map_exception(func, source=StopIteration, target=RuntimeError)
        if n_threads == 1:
            return MapPipe[R](self, func)
        else:
            return ThreadedMapPipe[R](self, func, n_threads)

    def do(
        self,
        func: Callable[[T], Any],
        n_threads: int = 1,
    ) -> "APipe[R]":
        """
        Run the func as side effect: the resulting Pipe forwards the upstream elements after func execution's end.

        Args:
            func (Callable[[T], R]): The function to be applied to each element.
            n_threads (int): The number of threads for concurrent execution (default is 1, meaning only the main thread is used).
        Returns:
            Pipe[T]: A new Pipe instance with elements resulting from applying the function to each element.
        """
        return self.map(_util.sidify(func), n_threads)

    def flatten(
        self: "APipe[Iterator[R]]",
        n_threads: int = 1,
    ) -> "APipe[R]":
        """
        Flatten the elements of the Pipe, which are assumed to be iterators, creating a new Pipe with individual elements.

        Returns:
            Pipe[R]: A new Pipe instance with individual elements obtained by flattening the original elements.
            n_threads (int): The number of threads for concurrent execution (default is 1, meaning only the main thread is used).
        """
        APipe.sanitize_n_threads(n_threads)
        if n_threads == 1:
            return FlattenPipe[R](self)
        else:
            return ThreadedFlattenPipe[R](self, n_threads)

    def chain(self, *others: "APipe[T]") -> "APipe[T]":
        """
        Create a new Pipe by chaining the elements of this Pipe with the elements from other Pipes. The elements of a given Pipe are yielded after its predecessor Pipe is exhausted.

        Args:
            *others (Pipe[T]): One or more additional Pipe instances to chain with this Pipe.

        Returns:
            Pipe[T]: A new Pipe instance with elements from this Pipe followed by elements from other Pipes.
        """
        return ChainPipe[T](self, list(others))

    def filter(self, predicate: Callable[[T], bool]) -> "APipe[T]":
        """
        Filter the elements of the Pipe based on the given predicate, creating a new Pipe with filtered elements.

        Args:
            predicate (Callable[[T], bool]): The function that determines whether an element should be included.

        Returns:
            Pipe[T]: A new Pipe instance with elements that satisfy the predicate.
        """
        return FilterPipe[T](self, predicate)

    def batch(self, size: int = 100, period: float = float("inf")) -> "APipe[List[T]]":
        """
        Batch elements of the Pipe into lists of a specified size or within a specified time window.

        Args:
            size (int, optional): The maximum number of elements per batch (default is 100).
            period (float, optional): The maximum number of seconds to wait before yielding a batch (default is infinity).

        Returns:
            Pipe[List[T]]: A new Pipe instance with lists containing batches of elements.
        """
        return BatchPipe[List[T]](self, size, period)

    def slow(self, freq: float) -> "APipe[T]":
        """
        Slow down the iteration of elements in the Pipe, creating a new Pipe with a specified frequency.

        Args:
            freq (float): The frequency (in milliseconds) at which elements are iterated.

        Returns:
            Pipe[T]: A new Pipe instance with elements iterated at the specified frequency.
        """
        return SlowPipe[T](self, freq)

    def catch(self, *classes: Type[Exception], ignore=False) -> "APipe[T]":
        """
        Any error whose class is exception_class or a subclass of it will be catched and yielded.

        Args:
            exception_class (Type[Exception]): The class of exceptions to catch
            ingore (bool): If True then the encountered exception_class errors will be skipped.

        Returns:
            Pipe[T]: A new Pipe instance with error handling capability.
        """
        return CatchPipe[T](self, classes, ignore)

    def log(self, what: str = "elements") -> "APipe[T]":
        """
        Log the elements of the Pipe as they are iterated.

        Args:
            what (str): name the objects yielded by the pipe for clearer logs, must be a plural descriptor.

        Returns:
            Pipe[T]: A new Pipe instance with logging capability.
        """
        return LogPipe[T](self, what)

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
        if not isinstance(self, LogPipe):
            plan = self.log("output elements")
        else:
            plan = self
        error_samples: List[Exception] = []
        samples = (
            plan.catch(Exception, ignore=False)
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


class SourcePipe(APipe[T]):
    def __init__(self, source: Callable[[], Iterator[T]]):
        """
        Initialize a Pipe with a data source.

        The source must be a callable that returns an iterator, i.e., an object implementing __iter__ and __next__ methods.
        Each subsequent iteration over the pipe will use a fresh iterator obtained from `source()`.

        Args:
            source (Callable[[], Iterator[T]]): A factory function called to obtain a fresh data source iterator for each iteration.
        """
        super().__init__()
        if not isinstance(source, Callable):
            raise TypeError(
                f"source must be a callable returning an iterator, but the provided source is not a callable: got source '{source}' of type {type(source)}."
            )
        self.source = source

    def __iter__(self) -> Iterator[T]:
        iterator = self.source()
        try:
            # duck-type checks that the object returned by the source is an iterator
            iterator.__iter__
            iterator.__next__
        except AttributeError:
            raise TypeError(
                f"source must be a callable returning an iterator (implements __iter__ and __next__ methods), but the object resulting from a call to source() was not an iterator: got '{iterator}' of type {type(iterator)}."
            )
        return iterator


class FilterPipe(APipe[T]):
    def __init__(self, upstream: APipe[T], predicate: Callable[[T], bool]):
        super().__init__(upstream)
        self.predicate = predicate

    def __iter__(self) -> Iterator[T]:
        return filter(self.predicate, iter(self.upstream))


class MapPipe(APipe[R]):
    def __init__(self, upstream: APipe[T], func: Callable[[T], R]):
        super().__init__(upstream)
        self.func = func

    def __iter__(self) -> Iterator[R]:
        return map(self.func, iter(self.upstream))


class ThreadedMapPipe(APipe[R]):
    def __init__(self, upstream: APipe[T], func: Callable[[T], R], n_threads: int):
        super().__init__(upstream)
        self.func = func
        self.n_threads = n_threads

    def __iter__(self) -> Iterator[R]:
        return _concurrent_exec.ThreadedMappingIteratorWrapper(
            iter(self.upstream), self.func, n_workers=self.n_threads
        )


class LogPipe(APipe[T]):
    def __init__(self, upstream: APipe[T], what: str = "elements"):
        super().__init__(upstream)
        self.what = what

    def __iter__(self) -> Iterator[T]:
        return _exec.LoggingIteratorWrapper(iter(self.upstream), self.what)


class FlattenPipe(APipe[T]):
    def __init__(self, upstream: APipe[T]):
        super().__init__(upstream)

    def __iter__(self) -> Iterator[T]:
        return _exec.FlatteningIteratorWrapper(iter(self.upstream))


class ThreadedFlattenPipe(APipe[R]):
    def __init__(self, upstream: APipe[T], n_threads: int):
        super().__init__(upstream)
        self.n_threads = n_threads

    def __iter__(self) -> Iterator[R]:
        return _concurrent_exec.ThreadedFlatteningIteratorWrapper(
            iter(self.upstream), n_workers=self.n_threads
        )


class BatchPipe(APipe[T]):
    def __init__(self, upstream: APipe[T], size: int, period: float):
        super().__init__(upstream)
        self.size = size
        self.period = period

    def __iter__(self) -> Iterator[T]:
        return _exec.BatchingIteratorWrapper(
            iter(self.upstream), self.size, self.period
        )


class CatchPipe(APipe[T]):
    def __init__(
        self, upstream: APipe[T], classes: Tuple[Type[Exception]], ignore: bool
    ):
        super().__init__(upstream)
        self.classes = classes
        self.ignore = ignore

    def __iter__(self) -> Iterator[T]:
        return _exec.CatchingIteratorWrapper(
            iter(self.upstream), self.classes, self.ignore
        )


class ChainPipe(APipe[T]):
    def __init__(self, upstream: APipe[T], others: List[APipe]):
        super().__init__(upstream)
        self.others = others

    def __iter__(self) -> Iterator[T]:
        return itertools.chain(iter(self.upstream), *list(map(iter, self.others)))


class SlowPipe(APipe[T]):
    def __init__(self, upstream: APipe[T], freq: float):
        super().__init__(upstream)
        self.freq = freq

    def __iter__(self) -> Iterator[T]:
        return _exec.SlowingIteratorWrapper(iter(self.upstream), self.freq)
