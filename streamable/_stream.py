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

from streamable._util import (
    LOGGER,
    validate_batch_seconds,
    validate_batch_size,
    validate_concurrency,
    validate_slow_frequency,
)

if TYPE_CHECKING:
    import builtins

    from streamable._visitors._base import Visitor

U = TypeVar("U")
T = TypeVar("T")
V = TypeVar("V")


class Stream(Iterable[T]):
    def __init__(self, source: Callable[[], Iterable[T]]) -> None:
        """
        Initialize a Stream by providing a source iterable.

        Args:
            source (Callable[[], Iterator[T]]): The data source. This function is used to provide a fresh iterable to each iteration over the stream.
        """
        self.upstream: "Optional[Stream]" = None
        if not callable(source):
            raise TypeError(f"`source` must be a callable but got a {type(source)}")
        self.source = source

    def __add__(self, other: "Stream[T]") -> "Stream[T]":
        """
        a + b is syntax sugar for a.chain(b).
        """
        return self.chain(other)

    def __iter__(self) -> Iterator[T]:
        from streamable._visitors import _iteration

        return self._accept(_iteration.IteratorProducingVisitor[T]())

    def __str__(self) -> str:
        return f"Stream(source={self.source})"

    def _accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_source_stream(self)

    def batch(self, size: int, seconds: float = float("inf")) -> "Stream[List[T]]":
        """
        Yield upstream elements grouped in lists.
        A list will have ` size` elements unless:
        - an exception occurs upstream, the batch prior to the exception is yielded uncomplete.
        - the time elapsed since the last yield of a batch is greater than `seconds`.
        - upstream is exhausted.

        Args:
            size (int): Maximum number of elements per batch.
            seconds (float, optional): Maximum number of seconds between two yields (default is infinity).

        Returns:
            Stream[List[T]]: A stream of upstream elements batched into lists.
        """
        validate_batch_size(size)
        validate_batch_seconds(seconds)
        return BatchStream(self, size, seconds)

    def catch(
        self,
        *classes: Type[Exception],
        when: Optional[Callable[[Exception], bool]] = None,
        raise_at_exhaustion: bool = False,
    ) -> "Stream[T]":
        """
        Catches the upstream exceptions whose type is in `classes` and satisfying the `when` predicate if provided.

        Args:
            classes (Type[Exception]): The classes of exception to be catched.
            when (Callable[[Exception], bool], optional): An additional condition that must be satisfied to catch the exception.
            raise_at_exhaustion (bool, optional): Set to True if you want the first catched exception to be raised when upstream is exhausted (default is False).

        Returns:
            Stream[T]: A stream of upstream elements catching the eligible exceptions.
        """
        return CatchStream(
            self, *classes, when=when, raise_at_exhaustion=raise_at_exhaustion
        )

    def chain(self, *others: "Stream[T]") -> "Stream[T]":
        """
        Yield the elements of the chained streams, in order.
        The elements of a given stream are yielded after its predecessor is exhausted.

        Args:
            *others (Stream[T]): One or more streams to chain with this stream.

        Returns:
            Stream[T]: A stream of elements of each stream in the chain, in order.
        """
        return ChainStream(self, list(others))

    def do(
        self,
        func: Callable[[T], Any],
        concurrency: int = 1,
    ) -> "Stream[T]":
        """
        Call `func` on upstream elements, discarding the result and yielding upstream elements unchanged and in order.
        If `func(elem)` throws an exception, then this exception will be thrown when iterating over the stream and `elem` will not be yielded.

        Args:
            func (Callable[[T], Any]): The function to be applied to each element.
            concurrency (int): The number of threads used to concurrently apply the function (default is 1, meaning no concurrency).
        Returns:
            Stream[T]: A stream of upstream elements, unchanged.
        """
        validate_concurrency(concurrency)
        return DoStream(self, func, concurrency)

    def exhaust(self, explain: bool = False) -> int:
        """
        Iterates over the stream until exhaustion.

        Args:
            explain (bool, optional): Set to True to print the explain plan before the iteration (default in False).
        Returns:
            int: The number of elements that have been yielded by the stream.
        """
        if explain:
            LOGGER.info(self.explain())
        yields = 0
        for _ in self:
            yields += 1
        return yields

    def explain(self, colored: bool = False) -> str:
        """
        Returns a friendly representation of this stream operations.
        """
        from streamable._visitors import _explanation

        return self._accept(_explanation.ExplainingVisitor(colored))

    def filter(self, predicate: Callable[[T], bool]) -> "Stream[T]":
        """
        Filter the elements of the stream based on the given predicate.

        Args:
            predicate (Callable[[T], bool]): The function that decides whether an element should be kept or not.

        Returns:
            Stream[T]: A stream of upstream elements satisfying the predicate.
        """
        return FilterStream(self, predicate)

    @overload
    def flatten(
        self: "Stream[Iterable[U]]",
        concurrency: int = 1,
    ) -> "Stream[U]":
        ...

    @overload
    def flatten(
        self: "Stream[Collection[U]]",
        concurrency: int = 1,
    ) -> "Stream[U]":
        ...

    @overload
    def flatten(
        self: "Stream[Stream[U]]",
        concurrency: int = 1,
    ) -> "Stream[U]":
        ...

    @overload
    def flatten(
        self: "Stream[Iterator[U]]",
        concurrency: int = 1,
    ) -> "Stream[U]":
        ...

    @overload
    def flatten(
        self: "Stream[List[U]]",
        concurrency: int = 1,
    ) -> "Stream[U]":
        ...

    @overload
    def flatten(
        self: "Stream[Sequence[U]]",
        concurrency: int = 1,
    ) -> "Stream[U]":
        ...

    @overload
    def flatten(
        self: "Stream[builtins.map[U]]",
        concurrency: int = 1,
    ) -> "Stream[U]":
        ...

    @overload
    def flatten(
        self: "Stream[builtins.filter[U]]",
        concurrency: int = 1,
    ) -> "Stream[U]":
        ...

    @overload
    def flatten(
        self: "Stream[Set[U]]",
        concurrency: int = 1,
    ) -> "Stream[U]":
        ...

    def flatten(
        self: "Stream[Iterable[U]]",
        concurrency: int = 1,
    ) -> "Stream[U]":
        """
        Iterate over upstream elements, assumed to be iterables, and individually yield the sub-elements.

        Args:
            concurrency (int): The number of threads used to concurrently flatten the upstream iterables (default is 1, meaning no concurrency).
        Returns:
            Stream[R]: A stream of flattened elements from upstream iterables.
        """
        validate_concurrency(concurrency)
        return FlattenStream(self, concurrency)

    def map(
        self,
        func: Callable[[T], U],
        concurrency: int = 1,
    ) -> "Stream[U]":
        """
        Apply `func` to the upstream elements and yield the results in order.

        Args:
            func (Callable[[T], R]): The function to be applied to each element.
            concurrency (int): The number of threads used to concurrently apply the function (default is 1, meaning no concurrency).
        Returns:
            Stream[R]: A stream of results of `func` applied to upstream elements.
        """
        validate_concurrency(concurrency)
        return MapStream(self, func, concurrency)

    def observe(self, what: str = "elements", colored: bool = False) -> "Stream[T]":
        """
        Logs the evolution of the iteration over elements.

        A logarithmic scale is used to prevent logs flood:
        - a 1st log is produced for the yield of the 1st element
        - a 2nd log is produced when we reach the 2nd element
        - a 3rd log is produced when we reach the 4th element
        - a 4th log is produced when we reach the 8th element
        - ...

        Args:
            what (str): (plural) name representing the objects yielded.
            colored (bool): whether or not to use ascii colorization.

        Returns:
            Stream[T]: A stream of upstream elements whose iteration is logged for observability.
        """
        return ObserveStream(self, what, colored)

    def slow(self, frequency: float) -> "Stream[T]":
        """
        Slow down the iteration down to a maximum `frequency` = maximum number of elements yielded per second.

        Args:
            frequency (float): The maximum number of elements yielded per second.

        Returns:
            Stream[T]: A stream yielding upstream elements at a maximum `frequency`.
        """
        validate_slow_frequency(frequency)
        return SlowStream(self, frequency)


class BatchStream(Stream[List[T]]):
    def __init__(self, upstream: Stream[T], size: int, seconds: float):
        self.upstream: Stream[T] = upstream
        self.size = size
        self.seconds = seconds

    def _accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_batch_stream(self)

    def __str__(self) -> str:
        return f"BatchStream(size={self.size}, seconds={self.seconds})"


class CatchStream(Stream[T]):
    def __init__(
        self,
        upstream: Stream[T],
        *classes: Type[Exception],
        when: Optional[Callable[[Exception], bool]],
        raise_at_exhaustion: bool,
    ):
        self.upstream: Stream[T] = upstream
        self.classes = classes
        self.when = when
        self.raise_at_exhaustion = raise_at_exhaustion

    def _accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_catch_stream(self)

    def __str__(self) -> str:
        return f"CatchStream(classes={list(map(lambda o: o.__name__, self.classes))}, when={self.when}, raise_at_exhaustion={self.raise_at_exhaustion})"


class ChainStream(Stream[T]):
    def __init__(self, upstream: Stream[T], others: List[Stream]):
        self.upstream: Stream[T] = upstream
        self.others = others

    def _accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_chain_stream(self)

    def __str__(self) -> str:
        return f"ChainStream(others=[{len(self.others)} other streams])"


class DoStream(Stream[T]):
    def __init__(self, upstream: Stream[T], func: Callable[[T], Any], concurrency: int):
        self.upstream: Stream[T] = upstream
        self.func = func
        self.concurrency = concurrency

    def _accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_do_stream(self)

    def __str__(self) -> str:
        return f"DoStream(func={self.func}, concurrency={self.concurrency})"


class FilterStream(Stream[T]):
    def __init__(self, upstream: Stream[T], predicate: Callable[[T], bool]):
        self.upstream: Stream[T] = upstream
        self.predicate = predicate

    def _accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_filter_stream(self)

    def __str__(self) -> str:
        return f"FilterStream(predicate={self.predicate})"


class FlattenStream(Stream[T]):
    def __init__(self, upstream: Stream[Iterable[T]], concurrency: int) -> None:
        self.upstream: Stream[Iterable[T]] = upstream
        self.concurrency = concurrency

    def _accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_flatten_stream(self)

    def __str__(self) -> str:
        return f"FlattenStream(concurrency={self.concurrency})"


class ObserveStream(Stream[T]):
    def __init__(self, upstream: Stream[T], what: str, colored: bool):
        self.upstream: Stream[T] = upstream
        self.what = what
        self.colored = colored

    def _accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_observe_stream(self)

    def __str__(self) -> str:
        return f"ObserveStream(what='{self.what}', colored={self.colored})"


class MapStream(Stream[U], Generic[T, U]):
    def __init__(self, upstream: Stream[T], func: Callable[[T], U], concurrency: int):
        self.upstream: Stream[T] = upstream
        self.func = func
        self.concurrency = concurrency

    def _accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_map_stream(self)

    def __str__(self) -> str:
        return f"MapStream(func={self.func}, concurrency={self.concurrency})"


class SlowStream(Stream[T]):
    def __init__(self, upstream: Stream[T], frequency: float):
        self.upstream: Stream[T] = upstream
        self.frequency = frequency

    def _accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_slow_stream(self)

    def __str__(self) -> str:
        return f"SlowStream(frequency={self.frequency})"
