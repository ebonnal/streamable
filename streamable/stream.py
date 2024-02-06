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
    TypeVar,
    cast,
    overload,
)

from streamable._util import (
    LOGGER,
    get_name,
    validate_batch_seconds,
    validate_batch_size,
    validate_concurrency,
    validate_limit_count,
    validate_slow_frequency,
)

if TYPE_CHECKING:
    import builtins

    from streamable.visitor import Visitor

U = TypeVar("U")
T = TypeVar("T")
V = TypeVar("V")


class Stream(Iterable[T]):
    def __init__(self, source: Callable[[], Iterable[T]]) -> None:
        """
        Initialize a Stream with a source iterable.

        Args:
            source (Callable[[], Iterable[T]]): Function to be called at iteration to get the stream's source iterable.
        """
        if not callable(source):
            raise TypeError(f"`source` must be a callable but got a {type(source)}")
        self._source = source

    upstream: "Optional[Stream]" = None
    "Optional[Stream]: Parent stream if any."

    def __add__(self, other: "Stream[T]") -> "Stream[T]":
        """
        a + b is syntax suger for Stream(lambda: [a, b]).flatten().
        """
        return cast(Stream[T], Stream([self, other].__iter__).flatten())

    def __iter__(self) -> Iterator[T]:
        from streamable.visitors.iteration import IterationVisitor

        return self.accept(IterationVisitor[T]())

    def __repr__(self) -> str:
        return f"Stream(source={get_name(self._source)})"

    def accept(self, visitor: "Visitor[V]") -> V:
        """
        Entry point to visit this stream (en.wikipedia.org/wiki/Visitor_pattern).
        """
        return visitor.visit_stream(self)

    def batch(self, size: int, seconds: float = float("inf")) -> "Stream[List[T]]":
        """
        Yield upstream elements grouped in lists.
        A list will have ` size` elements unless:
        - an exception occurs upstream={get_object_name(self.upstream)}, the batch prior to the exception is yielded uncomplete.
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
        predicate: Callable[[Exception], Any] = bool,
        raise_at_exhaustion: bool = False,
    ) -> "Stream[T]":
        """
        Catches the upstream exceptions which are satisfying the provided `predicate`.

        Args:
            predicate (Callable[[Exception], Any], optional): The exception will be catched if `predicate(exception)` is Truthy (all exceptions catched by default).
            raise_at_exhaustion (bool, optional): Set to True if you want the first catched exception to be raised when upstream is exhausted (default is False).

        Returns:
            Stream[T]: A stream of upstream elements catching the eligible exceptions.
        """
        return CatchStream(self, predicate, raise_at_exhaustion=raise_at_exhaustion)

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
        from streamable.visitors import explanation

        return self.accept(explanation.ExplanationVisitor(colored))

    def filter(self, predicate: Callable[[T], Any] = bool) -> "Stream[T]":
        """
        Filter the elements of the stream based on the given predicate.

        Args:
            predicate (Callable[[T], Any], optional): Keep element if `predicate(elem)` is Truthy (default keeps Truthy elements).

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

    def foreach(
        self,
        func: Callable[[T], Any],
        concurrency: int = 1,
    ) -> "Stream[T]":
        """
        Call `func` on upstream elements and yield them in order.
        If `func(elem)` throws an exception then it will be thrown and `elem` will not be yielded.

        Args:
            func (Callable[[T], Any]): The function to be applied to each element.
            concurrency (int): The number of threads used to concurrently apply the function (default is 1, meaning no concurrency).
        Returns:
            Stream[T]: A stream of upstream elements, unchanged.
        """
        validate_concurrency(concurrency)
        return ForeachStream(self, func, concurrency)

    def limit(self, count: int) -> "Stream[T]":
        """
        Truncate to first `count` elements.

        Args:
            count (int): The maximum number of elements to yield.

        Returns:
            Stream[T]: A stream of `count` upstream elements.
        """
        validate_limit_count(count)
        return LimitStream(self, count)

    def map(
        self,
        func: Callable[[T], U],
        concurrency: int = 1,
    ) -> "Stream[U]":
        """
        Apply `func` on upstream elements and yield the results in order.

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
        Logs the progress of any iteration over this stream's elements.

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
        Slow down the iteration down to a maximum `frequency`, more precisely an element will only be yielded if a period of 1/frequency seconds has elapsed since the last yield.

        Args:
            frequency (float): Maximum yields per second.

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

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_batch_stream(self)

    def __repr__(self) -> str:
        return f"BatchStream(upstream={get_name(self.upstream)}, size={self.size}, seconds={self.seconds})"


class CatchStream(Stream[T]):
    def __init__(
        self,
        upstream: Stream[T],
        predicate: Callable[[Exception], Any],
        raise_at_exhaustion: bool,
    ):
        self.upstream: Stream[T] = upstream
        self.predicate = predicate
        self.raise_at_exhaustion = raise_at_exhaustion

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_catch_stream(self)

    def __repr__(self) -> str:
        return f"CatchStream(upstream={get_name(self.upstream)}, predicate={get_name(self.predicate)}, raise_at_exhaustion={self.raise_at_exhaustion})"


class FilterStream(Stream[T]):
    def __init__(self, upstream: Stream[T], predicate: Callable[[T], Any]):
        self.upstream: Stream[T] = upstream
        self.predicate = predicate

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_filter_stream(self)

    def __repr__(self) -> str:
        return f"FilterStream(upstream={get_name(self.upstream)}, predicate={get_name(self.predicate)})"


class FlattenStream(Stream[T]):
    def __init__(self, upstream: Stream[Iterable[T]], concurrency: int) -> None:
        self.upstream: Stream[Iterable[T]] = upstream
        self.concurrency = concurrency

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_flatten_stream(self)

    def __repr__(self) -> str:
        return f"FlattenStream(upstream={get_name(self.upstream)}, concurrency={self.concurrency})"


class ForeachStream(Stream[T]):
    def __init__(self, upstream: Stream[T], func: Callable[[T], Any], concurrency: int):
        self.upstream: Stream[T] = upstream
        self.func = func
        self.concurrency = concurrency

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_foreach_stream(self)

    def __repr__(self) -> str:
        return f"ForeachStream(upstream={get_name(self.upstream)}, func={get_name(self.func)}, concurrency={self.concurrency})"


class LimitStream(Stream[T]):
    def __init__(self, upstream: Stream[T], count: int) -> None:
        self.upstream: Stream[T] = upstream
        self.count = count

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_limit_stream(self)

    def __repr__(self) -> str:
        return f"LimitStream(upstream={get_name(self.upstream)}, count={self.count})"


class MapStream(Stream[U], Generic[T, U]):
    def __init__(self, upstream: Stream[T], func: Callable[[T], U], concurrency: int):
        self.upstream: Stream[T] = upstream
        self.func = func
        self.concurrency = concurrency

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_map_stream(self)

    def __repr__(self) -> str:
        return f"MapStream(upstream={get_name(self.upstream)}, func={get_name(self.func)}, concurrency={self.concurrency})"


class ObserveStream(Stream[T]):
    def __init__(self, upstream: Stream[T], what: str, colored: bool):
        self.upstream: Stream[T] = upstream
        self.what = what
        self.colored = colored

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_observe_stream(self)

    def __repr__(self) -> str:
        return f"ObserveStream(upstream={get_name(self.upstream)}, what='{self.what}', colored={self.colored})"


class SlowStream(Stream[T]):
    def __init__(self, upstream: Stream[T], frequency: float):
        self.upstream: Stream[T] = upstream
        self.frequency = frequency

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_slow_stream(self)

    def __repr__(self) -> str:
        return f"SlowStream(upstream={get_name(self.upstream)}, frequency={self.frequency})"
