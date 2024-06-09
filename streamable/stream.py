from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    Coroutine,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    TypeVar,
    Union,
    cast,
    overload,
)

from streamable._util import (
    LOGGER,
    validate_concurrency,
    validate_group_seconds,
    validate_group_size,
    validate_iterable,
    validate_limit_count,
    validate_slow_frequency,
)

# fmt: off
if TYPE_CHECKING: import builtins
if TYPE_CHECKING: from streamable.visitor import Visitor
# fmt: on

U = TypeVar("U")
T = TypeVar("T")
V = TypeVar("V")


class Stream(Iterable[T]):
    # fmt: off
    @overload
    def __init__(self, source: Iterable[T]) -> None: ...
    @overload
    def __init__(self, source: Callable[[], Iterable[T]]) -> None: ...
    # fmt: on

    def __init__(self, source: Union[Iterable[T], Callable[[], Iterable[T]]]) -> None:
        """
        Initialize a Stream with a data source.

        Args:
            source (Union[Iterable[T], Callable[[], Iterable[T]]]): a source iterable or a function returning one (called for each new iteration on this stream).
        """
        if not callable(source):
            try:
                validate_iterable(source)
            except TypeError:
                raise TypeError(
                    "`source` must be either a Callable[[], Iterator] or an Iterable, but got a <class 'int'>"
                )
        self._source = source
        self._upstream: "Optional[Stream]" = None

    @property
    def upstream(self) -> "Optional[Stream]":
        """
        Returns:
            Optional[Stream]: Parent stream if any.
        """
        return self._upstream

    @property
    def source(self) -> Union[Iterable, Callable[[], Iterable]]:
        """
        Returns:
            Callable[[], Iterable]: Function to be called at iteration to get the stream's source iterable.
        """
        return self._source

    def __add__(self, other: "Stream[T]") -> "Stream[T]":
        """
        a + b is syntax suger for Stream(lambda: [a, b]).flatten().
        """
        return cast(Stream[T], Stream([self, other].__iter__).flatten())

    def __iter__(self) -> Iterator[T]:
        from streamable.visitors.iterator import IteratorVisitor

        return self.accept(IteratorVisitor[T]())

    def exhaust(self) -> int:
        """
        Iterate over the stream until exhaustion and count the elements yielded.

        Returns:
            int: The number of elements that have been yielded by the stream.
        """
        return sum(1 for _ in self)

    def accept(self, visitor: "Visitor[V]") -> V:
        """
        Entry point to visit this stream (en.wikipedia.org/wiki/Visitor_pattern).
        """
        return visitor.visit_stream(self)

    def catch(
        self,
        predicate: Callable[[Exception], Any] = bool,
        raise_at_exhaustion: bool = False,
    ) -> "Stream[T]":
        """
        Catch the upstream exceptions which are satisfying the provided `predicate`.

        Args:
            predicate (Callable[[Exception], Any], optional): The exception will be catched if `predicate(exception)` is Truthy (all exceptions catched by default).
            raise_at_exhaustion (bool, optional): Set to True if you want the first catched exception to be raised when upstream is exhausted (default is False).

        Returns:
            Stream[T]: A stream of upstream elements catching the eligible exceptions.
        """
        return CatchStream(self, predicate, raise_at_exhaustion=raise_at_exhaustion)

    def explain(self, colored: bool = False) -> "Stream[T]":
        """
        Log this stream's explanation (INFO level)
        """
        LOGGER.info(self.explanation(colored))
        return self

    def explanation(self, colored: bool = False) -> str:
        """
        Returns:
            str: A pretty representation of this stream's operations.
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

    # fmt: off
    @overload
    def flatten(
        self: "Stream[Iterable[U]]",
        concurrency: int = 1,
    ) -> "Stream[U]": ...

    @overload
    def flatten(
        self: "Stream[Collection[U]]",
        concurrency: int = 1,
    ) -> "Stream[U]": ...

    @overload
    def flatten(
        self: "Stream[Stream[U]]",
        concurrency: int = 1,
    ) -> "Stream[U]": ...

    @overload
    def flatten(
        self: "Stream[Iterator[U]]",
        concurrency: int = 1,
    ) -> "Stream[U]": ...

    @overload
    def flatten(
        self: "Stream[List[U]]",
        concurrency: int = 1,
    ) -> "Stream[U]": ...

    @overload
    def flatten(
        self: "Stream[Sequence[U]]",
        concurrency: int = 1,
    ) -> "Stream[U]": ...

    @overload
    def flatten(
        self: "Stream[builtins.map[U]]",
        concurrency: int = 1,
    ) -> "Stream[U]": ...

    @overload
    def flatten(
        self: "Stream[builtins.filter[U]]",
        concurrency: int = 1,
    ) -> "Stream[U]": ...

    @overload
    def flatten(
        self: "Stream[Set[U]]",
        concurrency: int = 1,
    ) -> "Stream[U]": ...
    # fmt: on

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

    def aforeach(
        self,
        func: Callable[[T], Coroutine],
        concurrency: int = 1,
    ) -> "Stream[T]":
        """
        Call the asynchronous `func` on upstream elements and yield them in order.
        If the `func(elem)` coroutine throws an exception then it will be thrown and `elem` will not be yielded.

        Args:
            func (Callable[[T], Any]): The asynchronous function to be applied to each element.
            concurrency (int): How many asyncio tasks will run at the same time.
        Returns:
            Stream[T]: A stream of upstream elements, unchanged.
        """
        validate_concurrency(concurrency)
        return AForeachStream(self, func, concurrency)

    def group(
        self,
        size: Optional[int] = None,
        seconds: float = float("inf"),
        by: Optional[Callable[[T], Any]] = None,
    ) -> "Stream[List[T]]":
        """
        Yield upstream elements grouped in lists.
        A group is a list of `size` elements for which `by` returns the same value, but it may contain fewer elements in these cases:
        - `seconds` have elapsed since the last yield of a group
        - upstream is exhausted
        - upstream raises an exception

        Args:
            size (Optional[int], optional): Maximum number of elements per group (default is infinity).
            seconds (float, optional): Maximum number of seconds between two yields (default is infinity).
            by (Optional[Callable[[T], Any]], optional): to cogroup elements for which this function returns to the same value. (default does not cogroup).

        Returns:
            Stream[List[T]]: A stream of upstream elements grouped into lists.
        """
        validate_group_size(size)
        validate_group_seconds(seconds)
        return GroupStream(self, size, seconds, by)

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

    def amap(
        self,
        func: Callable[[T], Coroutine[Any, Any, U]],
        concurrency: int = 1,
    ) -> "Stream[U]":
        """
        Apply an asynchronous `func` on upstream elements and yield the results in order.

        Args:
            func (Callable[[T], Coroutine[Any, Any, U]]): The asynchronous function to be applied to each element.
            concurrency (int): How many asyncio tasks will run at the same time.
        Returns:
            Stream[R]: A stream of results of `func` applied to upstream elements.
        """
        validate_concurrency(concurrency)
        return AMapStream(self, func, concurrency)

    def observe(self, what: str = "elements", colored: bool = False) -> "Stream[T]":
        """
        Log the progress of any iteration over this stream's elements.

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


class DownStream(Stream[U], Generic[T, U]):
    """
    Stream that has an upstream.
    """

    def __init__(self, upstream: Stream[T]) -> None:
        Stream.__init__(self, upstream.source)
        self._upstream: Stream[T] = upstream

    @property
    def upstream(self) -> Stream[T]:
        """
        Returns:
            Stream: Parent stream.
        """
        return self._upstream


class CatchStream(DownStream[T, T]):
    def __init__(
        self,
        upstream: Stream[T],
        predicate: Callable[[Exception], Any],
        raise_at_exhaustion: bool,
    ) -> None:
        super().__init__(upstream)
        self.predicate = predicate
        self.raise_at_exhaustion = raise_at_exhaustion

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_catch_stream(self)


class FilterStream(DownStream[T, T]):
    def __init__(self, upstream: Stream[T], predicate: Callable[[T], Any]) -> None:
        super().__init__(upstream)
        self.predicate = predicate

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_filter_stream(self)


class FlattenStream(DownStream[Iterable[T], T]):
    def __init__(self, upstream: Stream[Iterable[T]], concurrency: int) -> None:
        super().__init__(upstream)
        self.concurrency = concurrency

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_flatten_stream(self)


class ForeachStream(DownStream[T, T]):
    def __init__(
        self, upstream: Stream[T], func: Callable[[T], Any], concurrency: int
    ) -> None:
        super().__init__(upstream)
        self.func = func
        self.concurrency = concurrency

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_foreach_stream(self)


class AForeachStream(DownStream[T, T]):
    def __init__(
        self, upstream: Stream[T], func: Callable[[T], Coroutine], concurrency: int
    ) -> None:
        super().__init__(upstream)
        self.func = func
        self.concurrency = concurrency

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_aforeach_stream(self)


class GroupStream(DownStream[T, List[T]]):
    def __init__(
        self,
        upstream: Stream[T],
        size: Optional[int],
        seconds: float,
        by: Optional[Callable[[T], Any]],
    ) -> None:
        super().__init__(upstream)
        self.size = size
        self.seconds = seconds
        self.by = by

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_group_stream(self)


class LimitStream(DownStream[T, T]):
    def __init__(self, upstream: Stream[T], count: int) -> None:
        super().__init__(upstream)
        self.count = count

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_limit_stream(self)


class MapStream(DownStream[T, U]):
    def __init__(
        self, upstream: Stream[T], func: Callable[[T], U], concurrency: int
    ) -> None:
        super().__init__(upstream)
        self.func = func
        self.concurrency = concurrency

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_map_stream(self)


class AMapStream(DownStream[T, U]):
    def __init__(
        self,
        upstream: Stream[T],
        func: Callable[[T], Coroutine[Any, Any, U]],
        concurrency: int,
    ) -> None:
        super().__init__(upstream)
        self.func = func
        self.concurrency = concurrency

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_amap_stream(self)


class ObserveStream(DownStream[T, T]):
    def __init__(self, upstream: Stream[T], what: str, colored: bool) -> None:
        super().__init__(upstream)
        self.what = what
        self.colored = colored

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_observe_stream(self)


class SlowStream(DownStream[T, T]):
    def __init__(self, upstream: Stream[T], frequency: float) -> None:
        super().__init__(upstream)
        self.frequency = frequency

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_slow_stream(self)
