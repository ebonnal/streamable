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
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

from streamable.util import (
    get_logger,
    validate_concurrency,
    validate_group_seconds,
    validate_group_size,
    validate_iterable,
    validate_slow_frequency,
    validate_truncate_args,
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
        Initializes a Stream with a data source.

        Args:
            source (Union[Iterable[T], Callable[[], Iterable[T]]]): a source iterable or a function returning one (called for each new iteration on this stream).
        """
        if not callable(source):
            try:
                validate_iterable(source)
            except TypeError:
                raise TypeError(
                    "`source` must be either a Callable[[], Iterable] or an Iterable, but got a <class 'int'>"
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
        `a + b` returns a stream yielding all elements of `a`, followed by all elements of `b`.
        """
        return cast(Stream[T], Stream([self, other].__iter__).flatten())

    def __iter__(self) -> Iterator[T]:
        from streamable.visitors.iterator import IteratorVisitor

        return self.accept(IteratorVisitor[T]())

    def exhaust(self) -> int:
        """
        Iterates over the stream until exhaustion and counts the elements yielded.

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
        kind: Type[Exception] = Exception,
        finally_raise: bool = False,
    ) -> "Stream[T]":
        """
        Catches the upstream exceptions if they are instances of `kind`.

        Args:
            kind (Type[Exception], optional): The type of exceptions to catch (default is all non-exit exceptions).
            finally_raise (bool, optional): If True the first catched exception is raised when upstream's iteration ends (default is False).

        Returns:
            Stream[T]: A stream of upstream elements catching the eligible exceptions.
        """
        return CatchStream(self, kind, finally_raise)

    def explain(self) -> "Stream[T]":
        """
        Logs this stream's explanation (INFO level)
        """
        get_logger().info("explanation:\n%s", self.explanation())
        return self

    def explanation(self) -> str:
        """
        Returns:
            str: A pretty representation of this stream's operations.
        """
        from streamable.visitors import explanation

        return self.accept(explanation.ExplanationVisitor())

    def filter(self, keep: Callable[[T], Any] = bool) -> "Stream[T]":
        """
        Yields only upstream elements satisfying the `keep` predicate.

        Args:
            keep (Callable[[T], Any], optional): An element will be kept if `keep(elem)` is Truthy (default keeps Truthy elements).

        Returns:
            Stream[T]: A stream of upstream elements satisfying the `keep` predicate.
        """
        return FilterStream(self, keep)

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
        Iterates over upstream elements, assumed to be iterables, and individually yields the sub-elements.

        Args:
            concurrency (int): The number of threads used to concurrently flatten the upstream iterables (default is 1, meaning no concurrency).
        Returns:
            Stream[R]: A stream of flattened elements from upstream iterables.
        """
        validate_concurrency(concurrency)
        return FlattenStream(self, concurrency)

    def foreach(
        self,
        effect: Callable[[T], Any],
        concurrency: int = 1,
    ) -> "Stream[T]":
        """
        For each upstream element, yields it after having called `effect` on it.
        If `effect(elem)` throws an exception then it will be thrown and `elem` will not be yielded.

        Args:
            effect (Callable[[T], Any]): The function to be applied to each element as a side effect.
            concurrency (int): The number of threads used to concurrently apply the `effect` (default is 1, meaning no concurrency). Preserves the upstream order.
        Returns:
            Stream[T]: A stream of upstream elements, unchanged.
        """
        validate_concurrency(concurrency)
        return ForeachStream(self, effect, concurrency)

    def aforeach(
        self,
        effect: Callable[[T], Coroutine],
        concurrency: int = 1,
    ) -> "Stream[T]":
        """
        For each upstream element, yields it after having called the asynchronous `effect` on it.
        If the `effect(elem)` coroutine throws an exception then it will be thrown and `elem` will not be yielded.

        Args:
            effect (Callable[[T], Any]): The asynchronous function to be applied to each element as a side effect.
            concurrency (int): How many asyncio tasks will run at the same time. Preserves the upstream order.
        Returns:
            Stream[T]: A stream of upstream elements, unchanged.
        """
        validate_concurrency(concurrency)
        return AForeachStream(self, effect, concurrency)

    def group(
        self,
        size: Optional[int] = None,
        seconds: float = float("inf"),
        by: Optional[Callable[[T], Any]] = None,
    ) -> "Stream[List[T]]":
        """
        Yields upstream elements grouped into lists.
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

    def map(
        self,
        transformation: Callable[[T], U],
        concurrency: int = 1,
    ) -> "Stream[U]":
        """
        Applies `transformation` on upstream elements and yields the results.

        Args:
            transformation (Callable[[T], R]): The function to be applied to each element.
            concurrency (int): The number of threads used to concurrently apply `transformation` (default is 1, meaning no concurrency). Preserves the upstream order.
        Returns:
            Stream[R]: A stream of results of `transformation` applied to upstream elements.
        """
        validate_concurrency(concurrency)
        return MapStream(self, transformation, concurrency)

    def amap(
        self,
        transformation: Callable[[T], Coroutine[Any, Any, U]],
        concurrency: int = 1,
    ) -> "Stream[U]":
        """
        Applies the asynchrounous `transformation` on upstream elements and yields the results in order.

        Args:
            transformation (Callable[[T], Coroutine[Any, Any, U]]): The asynchronous function to be applied to each element.
            concurrency (int): How many asyncio tasks will run at the same time. Preserves the upstream order.
        Returns:
            Stream[R]: A stream of results of `transformation` applied to upstream elements.
        """
        validate_concurrency(concurrency)
        return AMapStream(self, transformation, concurrency)

    def observe(self, what: str = "elements") -> "Stream[T]":
        """
        Logs the progress of the iterations over this stream.

        A logarithmic scale is used to prevent logs flood:
        - a 1st log is produced for the yield of the 1st element
        - a 2nd log is produced when we reach the 2nd element
        - a 3rd log is produced when we reach the 4th element
        - a 4th log is produced when we reach the 8th element
        - ...

        Args:
            what (str): (plural) name representing the objects yielded.

        Returns:
            Stream[T]: A stream of upstream elements whose iteration's progress is logged.
        """
        return ObserveStream(self, what)

    def slow(self, frequency: float) -> "Stream[T]":
        """
        Slows the iteration down to a maximum `frequency`, more precisely an element will only be yielded if a period of `1/frequency` seconds has elapsed since the last yield.

        Args:
            frequency (float): Maximum yields per second.

        Returns:
            Stream[T]: A stream yielding upstream elements at a maximum `frequency`.
        """
        validate_slow_frequency(frequency)
        return SlowStream(self, frequency)

    def truncate(
        self, count: Optional[int] = None, when: Optional[Callable[[T], Any]] = None
    ) -> "Stream[T]":
        """
        Stops an iteration as soon as the `when` predicate is satisfied or `count` elements have been yielded.

        Args:
            count (int): The maximum number of elements to yield.
            when (Optional[Callable[[T], Any]], optional): Predicate function whose satisfaction stops an iteration, i.e. only elements for which `when(elem)` is Falsy will be yielded.

        Returns:
            Stream[T]: A stream of at most `count` upstream elements not satisfying the `when` predicate.
        """
        validate_truncate_args(count, when)
        return TruncateStream(self, count, when)


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
        kind: Type[Exception],
        finally_raise: bool,
    ) -> None:
        super().__init__(upstream)
        self.kind = kind
        self.finally_raise = finally_raise

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_catch_stream(self)


class FilterStream(DownStream[T, T]):
    def __init__(self, upstream: Stream[T], keep: Callable[[T], Any]) -> None:
        super().__init__(upstream)
        self.keep = keep

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
        self, upstream: Stream[T], effect: Callable[[T], Any], concurrency: int
    ) -> None:
        super().__init__(upstream)
        self.effect = effect
        self.concurrency = concurrency

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_foreach_stream(self)


class AForeachStream(DownStream[T, T]):
    def __init__(
        self, upstream: Stream[T], effect: Callable[[T], Coroutine], concurrency: int
    ) -> None:
        super().__init__(upstream)
        self.effect = effect
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


class MapStream(DownStream[T, U]):
    def __init__(
        self, upstream: Stream[T], transformation: Callable[[T], U], concurrency: int
    ) -> None:
        super().__init__(upstream)
        self.transformation = transformation
        self.concurrency = concurrency

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_map_stream(self)


class AMapStream(DownStream[T, U]):
    def __init__(
        self,
        upstream: Stream[T],
        transformation: Callable[[T], Coroutine[Any, Any, U]],
        concurrency: int,
    ) -> None:
        super().__init__(upstream)
        self.transformation = transformation
        self.concurrency = concurrency

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_amap_stream(self)


class ObserveStream(DownStream[T, T]):
    def __init__(self, upstream: Stream[T], what: str) -> None:
        super().__init__(upstream)
        self.what = what

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_observe_stream(self)


class SlowStream(DownStream[T, T]):
    def __init__(self, upstream: Stream[T], frequency: float) -> None:
        super().__init__(upstream)
        self.frequency = frequency

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_slow_stream(self)


class TruncateStream(DownStream[T, T]):
    def __init__(
        self,
        upstream: Stream[T],
        count: Optional[int] = None,
        when: Optional[Callable[[T], Any]] = None,
    ) -> None:
        super().__init__(upstream)
        self.count = count
        self.when = when

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_truncate_stream(self)
