from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Type,
    TypeVar,
    overload,
)

from streamable import _util

if TYPE_CHECKING:
    import builtins

    from streamable._visit._base import Visitor

R = TypeVar("R")
T = TypeVar("T")
V = TypeVar("V")


class Stream(Iterable[T]):
    _RUN_MAX_NUM_ERROR_SAMPLES = 8

    def __init__(self, source: Callable[[], Iterable[T]]) -> None:
        """
        Initialize a Stream with a data source.

        The source must be a callable that returns an iterator, i.e., an object implementing __iter__ and __next__ methods.
        Each subsequent iteration over the stream will use a fresh iterator obtained from `source()`.

        Args:
            source (Callable[[], Iterator[T]]): A factory function called to obtain a fresh data source iterator for each iteration.
        """
        self.upstream: "Optional[Stream]" = None
        if not callable(source):
            raise TypeError(f"source must be a callable but got a {type(source)}")
        self.source = source

    def __iter__(self) -> Iterator[T]:
        from streamable._visit import _iter

        return self._accept(_iter.IteratorProducingVisitor[T]())

    def __add__(self, other: "Stream[T]") -> "Stream[T]":
        return self.chain(other)

    def explain(self, colored: bool = False) -> str:
        from streamable._visit import _explanation

        return self._accept(_explanation.ExplainingVisitor(colored))

    def _accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_source_stream(self)

    @staticmethod
    def sanitize_concurrency(concurrency: int):
        if not isinstance(concurrency, int):
            raise TypeError(
                f"concurrency should be an int but got '{concurrency}' of type {type(concurrency)}."
            )
        if concurrency < 1:
            raise ValueError(
                f"concurrency should be greater or equal to 1, but got {concurrency}."
            )

    def map(
        self,
        func: Callable[[T], R],
        concurrency: int = 1,
    ) -> "Stream[R]":
        """
        Apply a function to each element of the Stream.

        Args:
            func (Callable[[T], R]): The function to be applied to each element.
            concurrency (int): The number of threads for concurrent execution (default is 1, meaning only the main thread is used).
        Returns:
            Stream[R]: A new Stream instance with elements resulting from applying the function to each element.
        """
        Stream.sanitize_concurrency(concurrency)
        return MapStream(self, func, concurrency)

    def do(
        self,
        func: Callable[[T], Any],
        concurrency: int = 1,
    ) -> "Stream[T]":
        """
        Run the func as side effect: the resulting Stream forwards the upstream elements after func execution's end.

        Args:
            func (Callable[[T], R]): The function to be applied to each element.
            concurrency (int): The number of threads for concurrent execution (default is 1, meaning only the main thread is used).
        Returns:
            Stream[T]: A new Stream instance with elements resulting from applying the function to each element.
        """
        Stream.sanitize_concurrency(concurrency)
        return DoStream(self, func, concurrency)

    @overload
    def flatten(
        self: "Stream[Iterable[R]]",
        concurrency: int = 1,
    ) -> "Stream[R]":
        ...

    @overload
    def flatten(
        self: "Stream[Collection[R]]",
        concurrency: int = 1,
    ) -> "Stream[R]":
        ...

    @overload
    def flatten(
        self: "Stream[Stream[R]]",
        concurrency: int = 1,
    ) -> "Stream[R]":
        ...

    @overload
    def flatten(
        self: "Stream[Iterator[R]]",
        concurrency: int = 1,
    ) -> "Stream[R]":
        ...

    @overload
    def flatten(
        self: "Stream[List[R]]",
        concurrency: int = 1,
    ) -> "Stream[R]":
        ...

    @overload
    def flatten(
        self: "Stream[Sequence[R]]",
        concurrency: int = 1,
    ) -> "Stream[R]":
        ...

    @overload
    def flatten(
        self: "Stream[builtins.map[R]]",
        concurrency: int = 1,
    ) -> "Stream[R]":
        ...

    @overload
    def flatten(
        self: "Stream[builtins.filter[R]]",
        concurrency: int = 1,
    ) -> "Stream[R]":
        ...

    @overload
    def flatten(
        self: "Stream[Set[R]]",
        concurrency: int = 1,
    ) -> "Stream[R]":
        ...

    def flatten(
        self: "Stream[Iterable[R]]",
        concurrency: int = 1,
    ) -> "Stream[R]":
        """
        Flatten the elements of the Stream, which are assumed to be iterables, creating a new Stream with individual elements.

        Returns:
            Stream[R]: A new Stream instance with individual elements obtained by flattening the original elements.
            concurrency (int): The number of threads for concurrent execution (default is 1, meaning only the main thread is used).
        """
        Stream.sanitize_concurrency(concurrency)
        return FlattenStream(self, concurrency)

    def chain(self, *others: "Stream[T]") -> "Stream[T]":
        """
        Create a new Stream by chaining the elements of this Stream with the elements from other Streams. The elements of a given Stream are yielded after its predecessor Stream is exhausted.

        Args:
            *others (Stream[T]): One or more additional Stream instances to chain with this Stream.

        Returns:
            Stream[T]: A new Stream instance with elements from this Stream followed by elements from other Streams.
        """
        return ChainStream(self, list(others))

    def filter(self, predicate: Callable[[T], bool]) -> "Stream[T]":
        """
        Filter the elements of the Stream based on the given predicate, creating a new Stream with filtered elements.

        Args:
            predicate (Callable[[T], bool]): The function that determines whether an element should be included.

        Returns:
            Stream[T]: A new Stream instance with elements that satisfy the predicate.
        """
        return FilterStream(self, predicate)

    def batch(
        self, size: int = 100, seconds: float = float("inf")
    ) -> "Stream[List[T]]":
        """
        Batch elements of the Stream into lists of a specified size or within a specified time window.

        Args:
            size (int, optional): The maximum number of elements per batch (default is 100).
            seconds (float, optional): The maximum number of seconds to wait before yielding a batch (default is infinity).

        Returns:
            Stream[List[T]]: A new Stream instance with lists containing batches of elements.
        """
        # if seconds == float("inf"):
        if size < 1:
            raise ValueError(
                f"batch's size should be >= 1."
            )
        if seconds <= 0:
            raise ValueError(
                f"batch's seconds should be > 0."
            )
        return BatchStream(self, size, seconds)

    def slow(self, frequency: float) -> "Stream[T]":
        """
        Slow down the iteration to a maximum frequency in Hz (max number of elements yielded per second).

        Args:
            frequency (float): The maximum frequency in Hz of the iteration, i.e. how many elements will be yielded per second at most.

        Returns:
            Stream[T]: A new Stream instance with elements iterated at the specified frequency.
        """
        if frequency <= 0:
            raise ValueError(
                f"frequency is the maximum number of elements to yield per second, it must be > 0."
            )
        return SlowStream(self, frequency)

    def catch(
        self,
        *classes: Type[Exception],
        when: Optional[Callable[[Exception], bool]] = None,
    ) -> "Stream[T]":
        """
        Any exception who is instance of `exception_class`will be catched, under the condition that the `when` predicate function (if provided) returns True.

        Args:
            classes (Type[Exception]): The class of exceptions to catch
            when (Callable[[Exception], bool], optional): catches an exception whose type is in `classes` only if this predicate function is None or evaluates to True.

        Returns:
            Stream[T]: A new Stream instance with error handling capability.
        """
        return CatchStream(self, *classes, when=when)

    def observe(self, what: str = "elements", colored: bool = False) -> "Stream[T]":
        """
        Will logs the evolution of the iteration over elements.

        Args:
            what (str): name the objects yielded by the stream for clearer logs, must be a plural descriptor.
            colored (bool): whether or not to use ascii colorization.

        Returns:
            Stream[T]: A new Stream instance with logging capability.
        """
        return ObserveStream(self, what, colored)

    def iterate(
        self,
        collect_limit: int = 0,
        raise_if_more_errors_than: int = 0,
        fail_fast: bool = False,
    ) -> List[T]:
        """
        Run the Stream:
        - iterates over it until it is exhausted,
        - logs
        - catches exceptions log a sample of them at the end of the iteration
        - raises the first encountered error if more exception than `raise_if_more_errors_than` are catched during iteration.
        - else returns a sample of the output elements

        Args:
            raise_if_more_errors_than (int, optional): An error will be raised if the number of encountered errors is more than this threshold (default is 0).
            collect_limit (int, optional): How many output elements to return (default is 0).
            fail_fast (bool, optional): Decide to raise at the first encountered exception or at the end of the iteration (default is False).
        Returns:
            List[T]: A list containing the elements of the Stream titeratecate to the first `n_samples` ones.
        Raises:
            Exception: If more exception than `raise_if_more_errors_than` are catched during iteration.
        """
        max_num_error_samples = self._RUN_MAX_NUM_ERROR_SAMPLES
        stream = self

        if not isinstance(self, ObserveStream):
            stream = self.observe("output elements")

        error_samples: List[Exception] = []
        errors_count = 0

        if not fail_fast:

            def register_error_sample(error):
                nonlocal errors_count
                errors_count += 1
                if len(error_samples) < max_num_error_samples:
                    error_samples.append(error)
                return True

            stream = stream.catch(Exception, when=register_error_sample)

        _util.LOGGER.info(stream.explain(colored=False))

        output_samples: List[T] = []
        for elem in stream:
            if len(output_samples) < collect_limit:
                output_samples.append(elem)

        if errors_count > 0:
            _util.LOGGER.error(
                "first %s error samples: %s\nWill now raise the first of them:",
                max_num_error_samples,
                list(map(repr, error_samples)),
            )
            if raise_if_more_errors_than < errors_count:
                raise error_samples[0]

        return output_samples


X = TypeVar("X")
Y = TypeVar("Y")
Z = TypeVar("Z")

class FilterStream(Stream[Y]):
    def __init__(self, upstream: Stream[Y], predicate: Callable[[Y], bool]):
        self.upstream: Stream[Y] = upstream
        self.predicate = predicate

    def _accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_filter_stream(self)


class MapStream(Stream[Z], Generic[Y, Z]):
    def __init__(self, upstream: Stream[Y], func: Callable[[Y], Z], concurrency: int):
        self.upstream: Stream[Y] = upstream
        self.func = func
        self.concurrency = concurrency

    def _accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_map_stream(self)


class DoStream(Stream[Y]):
    def __init__(self, upstream: Stream[Y], func: Callable[[Y], Any], concurrency: int):
        self.upstream: Stream[Y] = upstream
        self.func = func
        self.concurrency = concurrency

    def _accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_do_stream(self)


class ObserveStream(Stream[Y]):
    def __init__(self, upstream: Stream[Y], what: str, colored: bool):
        self.upstream: Stream[Y] = upstream
        self.what = what
        self.colored = colored

    def _accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_observe_stream(self)


class FlattenStream(Stream[Y]):
    def __init__(self, upstream: Stream[Iterable[Y]], concurrency: int) -> None:
        self.upstream: Stream[Iterable[Y]] = upstream
        self.concurrency = concurrency

    def _accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_flatten_stream(self)


class BatchStream(Stream[List[Y]]):
    def __init__(self, upstream: Stream[Y], size: int, seconds: float):
        self.upstream: Stream[Y] = upstream
        self.size = size
        self.seconds = seconds

    def _accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_batch_stream(self)


class CatchStream(Stream[Y]):
    def __init__(
        self,
        upstream: Stream[Y],
        *classes: Type[Exception],
        when: Optional[Callable[[Exception], bool]] = None,
    ):
        self.upstream: Stream[Y] = upstream
        self.classes = classes
        self.when = when

    def _accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_catch_stream(self)


class ChainStream(Stream[Y]):
    def __init__(self, upstream: Stream[Y], others: List[Stream]):
        self.upstream: Stream[Y] = upstream
        self.others = others

    def _accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_chain_stream(self)


class SlowStream(Stream[Y]):
    def __init__(self, upstream: Stream[Y], frequency: float):
        self.upstream: Stream[Y] = upstream
        self.frequency = frequency

    def _accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_slow_stream(self)
