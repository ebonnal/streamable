import copy
import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Collection,
    Coroutine,
    Dict,
    Generator,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

from streamable._utils._validation import (
    validate_concurrency,
    validate_errors,
    validate_group_size,
    validate_count_or_callable,
    validate_optional_positive_interval,
    validate_positive_interval,
    validate_via,
    validate_positive_count,
)

if TYPE_CHECKING:  # pragma: no cover
    import builtins

    from typing import Literal

    from typing_extensions import Concatenate, ParamSpec

    from streamable.visitors import Visitor

    P = ParamSpec("P")


U = TypeVar("U")
T = TypeVar("T")
V = TypeVar("V")


class Stream(Iterable[T], AsyncIterable[T], Awaitable["Stream[T]"]):
    __slots__ = ("_source", "_upstream")

    # fmt: off
    @overload
    def __init__(self, source: Iterable[T]) -> None: ...
    @overload
    def __init__(self, source: AsyncIterable[T]) -> None: ...
    @overload
    def __init__(self, source: Callable[[], Iterable[T]]) -> None: ...
    @overload
    def __init__(self, source: Callable[[], AsyncIterable[T]]) -> None: ...
    # fmt: on

    def __init__(
        self,
        source: Union[
            Iterable[T],
            Callable[[], Iterable[T]],
            AsyncIterable[T],
            Callable[[], AsyncIterable[T]],
        ],
    ) -> None:
        """
        A ``Stream[T]`` decorates an ``Iterable[T]`` or ``AsyncIterable[T]`` with a **fluent interface** enabling the chaining of lazy operations.

        Args:
            source (Union[Iterable[T], Callable[[], Iterable[T]], AsyncIterable[T], Callable[[], AsyncIterable[T]]]): The iterable to decorate. Can be specified via a function that will be called each time an iteration is started over the stream (i.e. for each call to ``iter(stream)``/``aiter(stream)``).
        """
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
    def source(
        self,
    ) -> Union[
        Iterable, Callable[[], Iterable], AsyncIterable, Callable[[], AsyncIterable]
    ]:
        """
        Returns:
            Union[Iterable, Callable[[], Iterable], AsyncIterable, Callable[[], AsyncIterable]]: The source of the stream's elements.
        """
        return self._source

    def __iter__(self) -> Iterator[T]:
        from streamable.visitors._iter import IteratorVisitor

        return self.accept(IteratorVisitor[T]())

    def __aiter__(self) -> AsyncIterator[T]:
        from streamable.visitors._aiter import AsyncIteratorVisitor

        return self.accept(AsyncIteratorVisitor[T]())

    def __eq__(self, other: Any) -> bool:
        """
        Checks if this stream is equal to ``other``, meaning they apply the same operations to the same source.

        Returns:
            bool: True if this stream is equal to ``other``.
        """
        from streamable.visitors._eq import EqualityVisitor

        return self.accept(EqualityVisitor(other))

    def __repr__(self) -> str:
        from streamable.visitors._repr import ReprVisitor

        return self.accept(ReprVisitor())

    def __str__(self) -> str:
        from streamable.visitors._repr import StrVisitor

        return self.accept(StrVisitor())

    def __call__(self) -> "Stream[T]":
        """
        Iterates over this stream until exhaustion.

        Returns:
            Stream[T]: self.
        """
        for _ in self:
            pass
        return self

    def __await__(self) -> Generator[int, None, "Stream[T]"]:
        """
        Iterates over this stream until exhaustion.

        Returns:
            Stream[T]: self.
        """
        yield from (self.acount().__await__())
        return self

    def __add__(self, other: "Stream[T]") -> "Stream[T]":
        """
        ``a + b`` returns a stream yielding all elements of ``a``, followed by all elements of ``b``.
        """
        return cast(Stream[T], Stream((self, other)).flatten())

    def accept(self, visitor: "Visitor[V]") -> V:
        """
        Entry point to visit this stream (en.wikipedia.org/wiki/Visitor_pattern).
        """
        return visitor.visit_stream(self)

    def count(self) -> int:
        """
        Iterates over this stream until exhaustion and returns the count of elements.

        Returns:
            int: Number of elements yielded during an entire iteration over this stream.
        """

        return sum(1 for _ in self)

    async def acount(self) -> int:
        """
        Iterates over this stream until exhaustion and returns the count of elements.

        Returns:
            int: Number of elements yielded during an entire iteration over this stream.
        """
        count = 0
        async for _ in self:
            count += 1
        return count

    def pipe(
        self,
        func: "Callable[Concatenate[Stream[T], P], U]",
        *args: "P.args",
        **kwargs: "P.kwargs",
    ) -> U:
        """
        Calls ``func``, with this stream as the first positional argument, optionally followed by ``*args`` and ``**kwargs``.

        Args:
            func (Callable[Concatenate[Stream[T], P], U]): The function to apply.
            *args (optional): Passed to ``func``.
            **kwargs (optional): Passed to ``func``.

        Returns:
            U: Result of ``func(self, *args, **kwargs)``.
        """
        return func(self, *args, **kwargs)

    def catch(
        self,
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        *,
        when: Optional[Callable[[Exception], Any]] = None,
        replace: Optional[Callable[[Exception], U]] = None,
        finally_raise: bool = False,
    ) -> "Stream[Union[T, U]]":
        """
        Catches the upstream exceptions if they are instances of ``errors`` type and they satisfy the ``when`` predicate.
        Optionally yields a replacement value (returned by ``replace(error)``).
        If any exception was caught during the iteration and ``finally_raise=True``, the first exception caught will be raised when the iteration finishes.

        Args:
            errors (Union[Type[Exception], Tuple[Type[Exception], ...]]): The exception types to catch.
            when (Optional[Callable[[Exception], Any]], optional): An additional condition that must be satisfied to catch the exception, i.e. ``when(exception)`` must be truthy. (default: no additional condition)
            replace (Optional[Callable[[Exception], U]], optional): ``replace(exception)`` will be yielded when an exception is caught. (default: do not yield any replacement value)
            finally_raise (bool, optional): If True the first exception caught is raised when upstream's iteration ends. (default: iteration ends without raising)

        Returns:
            Stream[Union[T, U]]: A stream of upstream elements catching the eligible exceptions.
        """
        validate_errors(errors)
        return CatchStream(
            self,
            errors,
            when=when,
            replace=replace,
            finally_raise=finally_raise,
        )

    def acatch(
        self,
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        *,
        when: Optional[Callable[[Exception], Coroutine[Any, Any, Any]]] = None,
        replace: Optional[Callable[[Exception], Coroutine[Any, Any, U]]] = None,
        finally_raise: bool = False,
    ) -> "Stream[Union[T, U]]":
        """
        Catches the upstream exceptions if they are instances of ``errors`` type and they satisfy the ``when`` predicate.
        Optionally yields a replacement value (returned by ``await replace(error)``).
        If any exception was caught during the iteration and ``finally_raise=True``, the first exception caught will be raised when the iteration finishes.

        Args:
            errors (Union[Type[Exception], Tuple[Type[Exception], ...]]): The exception types to catch.
            when (Optional[Callable[[Exception], Coroutine[Any, Any, Any]]], optional): An additional condition that must be satisfied to catch the exception, i.e. ``await when(exception)`` must be truthy. (default: no additional condition)
            replace (Optional[Callable[[Exception], Coroutine[Any, Any, U]]], optional): ``await replace(exception)`` will be yielded when an exception is caught. (default: do not yield any replacement value)
            finally_raise (bool, optional): If True the first exception caught is raised when upstream's iteration ends. (default: iteration ends without raising)

        Returns:
            Stream[Union[T, U]]: A stream of upstream elements catching the eligible exceptions.
        """
        validate_errors(errors)
        return ACatchStream(
            self,
            errors,
            when=when,
            replace=replace,
            finally_raise=finally_raise,
        )

    def distinct(
        self,
        by: Optional[Callable[[T], Any]] = None,
        *,
        consecutive: bool = False,
    ) -> "Stream[T]":
        """
        Filters the stream to yield only distinct elements.
        If a deduplication ``by`` is specified, ``foo`` and ``bar`` are treated as duplicates when ``by(foo) == by(bar)``.

        Among duplicates, the first encountered occurence in upstream order is yielded.

        Warning:
            During iteration, the distinct elements yielded are retained in memory to perform deduplication.
            Alternatively, remove only consecutive duplicates without memory footprint by setting ``consecutive=True``.

        Args:
            by (Callable[[T], Any], optional): Elements are deduplicated based on ``by(elem)``. (default: the deduplication is performed on the elements themselves)
            consecutive (bool, optional): Removes only consecutive duplicates if ``True``, or deduplicates globally if ``False``. (default: global deduplication)

        Returns:
            Stream: A stream containing only unique upstream elements.
        """
        return DistinctStream(self, by, consecutive)

    def adistinct(
        self,
        by: Optional[Callable[[T], Coroutine[Any, Any, Any]]] = None,
        *,
        consecutive: bool = False,
    ) -> "Stream[T]":
        """
        Filters the stream to yield only distinct elements.
        If a deduplication ``by`` is specified, ``foo`` and ``bar`` are treated as duplicates when ``await by(foo) == await by(bar)``.

        Among duplicates, the first encountered occurence in upstream order is yielded.

        Warning:
            During iteration, the distinct elements yielded are retained in memory to perform deduplication.
            Alternatively, remove only consecutive duplicates without memory footprint by setting ``consecutive=True``.

        Args:
            by (Callable[[T], Coroutine[Any, Any, Any]], optional): Elements are deduplicated based on ``await by(elem)``. (default: the deduplication is performed on the elements themselves)
            consecutive (bool, optional): Whether to deduplicate only consecutive duplicates, or globally. (default: the deduplication is global)

        Returns:
            Stream: A stream containing only unique upstream elements.
        """
        return ADistinctStream(self, by, consecutive)

    def filter(self, where: Callable[[T], Any]) -> "Stream[T]":
        """
        Filters the stream to yield only elements satisfying the ``where`` predicate.

        Args:
            where (Callable[[T], Any]): An element is kept if ``where(elem)`` is truthy.

        Returns:
            Stream[T]: A stream of upstream elements satisfying the `where` predicate.
        """
        return FilterStream(self, where)

    def afilter(self, where: Callable[[T], Coroutine[Any, Any, Any]]) -> "Stream[T]":
        """
        Filters the stream to yield only elements satisfying the ``where`` predicate.

        Args:
            where (Callable[[T], Coroutine[Any, Any, Any]], optional): An element is kept if ``await where(elem)`` is truthy. (default: keeps truthy elements)

        Returns:
            Stream[T]: A stream of upstream elements satisfying the `where` predicate.
        """
        return AFilterStream(self, where)

    # fmt: off
    @overload
    def flatten(
        self: "Stream[Iterable[U]]",
        *,
        concurrency: int = 1,
    ) -> "Stream[U]": ...

    @overload
    def flatten(
        self: "Stream[Collection[U]]",
        *,
        concurrency: int = 1,
    ) -> "Stream[U]": ...

    @overload
    def flatten(
        self: "Stream[Stream[U]]",
        *,
        concurrency: int = 1,
    ) -> "Stream[U]": ...

    @overload
    def flatten(
        self: "Stream[Iterator[U]]",
        *,
        concurrency: int = 1,
    ) -> "Stream[U]": ...

    @overload
    def flatten(
        self: "Stream[List[U]]",
        *,
        concurrency: int = 1,
    ) -> "Stream[U]": ...

    @overload
    def flatten(
        self: "Stream[Sequence[U]]",
        *,
        concurrency: int = 1,
    ) -> "Stream[U]": ...

    @overload
    def flatten(
        self: "Stream[builtins.map[U]]",
        *,
        concurrency: int = 1,
    ) -> "Stream[U]": ...

    @overload
    def flatten(
        self: "Stream[builtins.filter[U]]",
        *,
        concurrency: int = 1,
    ) -> "Stream[U]": ...

    @overload
    def flatten(
        self: "Stream[Set[U]]",
        *,
        concurrency: int = 1,
    ) -> "Stream[U]": ...

    @overload
    def flatten(
        self: "Stream[range]",
        *,
        concurrency: int = 1,
    ) -> "Stream[int]": ...
    # fmt: on

    def flatten(self: "Stream[Iterable[U]]", *, concurrency: int = 1) -> "Stream[U]":
        """
        Iterates over upstream elements assumed to be iterables, and individually yields their items.

        Args:
            concurrency (int, optional): Number of upstream iterables concurrently flattened via threads. (default: no concurrency)
        Returns:
            Stream[R]: A stream of flattened elements from upstream iterables.
        """
        validate_concurrency(concurrency)
        return FlattenStream(self, concurrency)

    # fmt: off
    @overload
    def aflatten(
        self: "Stream[AsyncIterator[U]]",
        *,
        concurrency: int = 1,
    ) -> "Stream[U]": ...

    @overload
    def aflatten(
        self: "Stream[AsyncIterable[U]]",
        *,
        concurrency: int = 1,
    ) -> "Stream[U]": ...
    # fmt: on

    def aflatten(
        self: "Stream[AsyncIterable[U]]", *, concurrency: int = 1
    ) -> "Stream[U]":
        """
        Iterates over upstream elements assumed to be async iterables, and individually yields their items.

        Args:
            concurrency (int, optional): Number of upstream async iterables concurrently flattened. (default: no concurrency)
        Returns:
            Stream[R]: A stream of flattened elements from upstream async iterables.
        """
        validate_concurrency(concurrency)
        return AFlattenStream(self, concurrency)

    def foreach(
        self,
        do: Callable[[T], Any],
        *,
        concurrency: int = 1,
        ordered: bool = True,
        via: "Literal['thread', 'process']" = "thread",
    ) -> "Stream[T]":
        """
        For each upstream element, yields it after having called ``do`` on it.
        If ``do(elem)`` throws an exception then it will be thrown and ``elem`` will not be yielded.

        Args:
            do (Callable[[T], Any]): The function to be applied to each element as a side effect.
            concurrency (int, optional): Represents both the number of threads used to concurrently apply the ``do`` and the size of the buffer containing not-yet-yielded elements. If the buffer is full, the iteration over the upstream is paused until an element is yielded from the buffer. (default: no concurrency)
            ordered (bool, optional): If ``concurrency`` > 1, whether to preserve the order of upstream elements or to yield them as soon as they are processed. (default: preserves upstream order)
            via ("thread" or "process", optional): If ``concurrency`` > 1, whether to apply ``to`` using processes or threads. (default: via threads)
        Returns:
            Stream[T]: A stream of upstream elements, unchanged.
        """
        validate_concurrency(concurrency)
        validate_via(via)
        return ForeachStream(self, do, concurrency, ordered, via)

    def aforeach(
        self,
        do: Callable[[T], Coroutine[Any, Any, Any]],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> "Stream[T]":
        """
        For each upstream element, yields it after having called the asynchronous ``do`` on it.
        If the ``await do(elem)`` coroutine throws an exception then it will be thrown and ``elem`` will not be yielded.

        Args:
            do (Callable[[T], Coroutine[Any, Any, Any]]): The asynchronous function to be applied to each element as a side effect.
            concurrency (int, optional): Represents both the number of async tasks concurrently applying the ``do`` and the size of the buffer containing not-yet-yielded elements. If the buffer is full, the iteration over the upstream is paused until an element is yielded from the buffer. (default: no concurrency)
            ordered (bool, optional): If ``concurrency`` > 1, whether to preserve the order of upstream elements or to yield them as soon as they are processed. (default: preserves upstream order)
        Returns:
            Stream[T]: A stream of upstream elements, unchanged.
        """
        validate_concurrency(concurrency)
        return AForeachStream(self, do, concurrency, ordered)

    def group(
        self,
        size: Optional[int] = None,
        *,
        interval: Optional[datetime.timedelta] = None,
        by: Optional[Callable[[T], Any]] = None,
    ) -> "Stream[List[T]]":
        """
        Groups upstream elements into lists.

        A group is yielded when any of the following conditions is met:
        - The group reaches ``size`` elements.
        - ``interval`` seconds have passed since the last group was yielded.
        - The upstream source is exhausted.

        If ``by`` is specified, groups will only contain elements sharing the same ``by(elem)`` value (see ``.groupby`` for ``(key, elements)`` pairs).

        Args:
            size (Optional[int], optional): The maximum number of elements per group. (default: no size limit)
            interval (float, optional): Yields a group if ``interval`` seconds have passed since the last group was yielded. (default: no interval limit)
            by (Optional[Callable[[T], Any]], optional): If specified, groups will only contain elements sharing the same ``by(elem)`` value. (default: does not co-group elements)
        Returns:
            Stream[List[T]]: A stream of upstream elements grouped into lists.
        """
        validate_group_size(size)
        validate_optional_positive_interval(interval, name="interval")
        return GroupStream(self, size, interval, by)

    def agroup(
        self,
        size: Optional[int] = None,
        *,
        interval: Optional[datetime.timedelta] = None,
        by: Optional[Callable[[T], Coroutine[Any, Any, Any]]] = None,
    ) -> "Stream[List[T]]":
        """
        Groups upstream elements into lists.

        A group is yielded when any of the following conditions is met:
        - The group reaches ``size`` elements.
        - ``interval`` seconds have passed since the last group was yielded.
        - The upstream source is exhausted.

        If ``by`` is specified, groups will only contain elements sharing the same ``await by(elem)`` value (see ``.agroupby`` for ``(key, elements)`` pairs).

        Args:
            size (Optional[int], optional): The maximum number of elements per group. (default: no size limit)
            interval (float, optional): Yields a group if ``interval`` seconds have passed since the last group was yielded. (default: no interval limit)
            by (Optional[Callable[[T], Coroutine[Any, Any, Any]]], optional): If specified, groups will only contain elements sharing the same ``await by(elem)`` value. (default: does not co-group elements)
        Returns:
            Stream[List[T]]: A stream of upstream elements grouped into lists.
        """
        validate_group_size(size)
        validate_optional_positive_interval(interval, name="interval")
        return AGroupStream(self, size, interval, by)

    def groupby(
        self,
        key: Callable[[T], U],
        *,
        size: Optional[int] = None,
        interval: Optional[datetime.timedelta] = None,
    ) -> "Stream[Tuple[U, List[T]]]":
        """
        Groups upstream elements into ``(key, elements)`` tuples.

        A group is yielded when any of the following conditions is met:
        - A group reaches ``size`` elements.
        - ``interval`` seconds have passed since the last group was yielded.
        - The upstream source is exhausted.

        Args:
            key (Callable[[T], U]): A function that returns the group key for an element.
            size (Optional[int], optional): The maximum number of elements per group. (default: no size limit)
            interval (Optional[datetime.timedelta], optional): If specified, yields a group if ``interval`` seconds have passed since the last group was yielded. (default: no interval limit)

        Returns:
            Stream[Tuple[U, List[T]]]: A stream of upstream elements grouped by key, as ``(key, elements)`` tuples.
        """
        return GroupbyStream(self, key, size, interval)

    def agroupby(
        self,
        key: Callable[[T], Coroutine[Any, Any, U]],
        *,
        size: Optional[int] = None,
        interval: Optional[datetime.timedelta] = None,
    ) -> "Stream[Tuple[U, List[T]]]":
        """
        Groups upstream elements into ``(key, elements)`` tuples.

        A group is yielded when any of the following conditions is met:
        - A group reaches ``size`` elements.
        - ``interval`` seconds have passed since the last group was yielded.
        - The upstream source is exhausted.

        Args:
            key (Callable[[T], Coroutine[Any, Any, U]]): An async function that returns the group key for an element.
            size (Optional[int], optional): The maximum number of elements per group. (default: no size limit)
            interval (Optional[datetime.timedelta], optional): If specified, yields a group if ``interval`` seconds have passed since the last group was yielded. (default: no interval limit)

        Returns:
            Stream[Tuple[U, List[T]]]: A stream of upstream elements grouped by key, as ``(key, elements)`` tuples.
        """
        return AGroupbyStream(self, key, size, interval)

    def map(
        self,
        to: Callable[[T], U],
        *,
        concurrency: int = 1,
        ordered: bool = True,
        via: "Literal['thread', 'process']" = "thread",
    ) -> "Stream[U]":
        """
        Applies ``to`` on upstream elements and yields the results.

        Args:
            to (Callable[[T], R]): The async transformation to be applied to each element.
            concurrency (int, optional): Represents both the number of threads used to concurrently apply ``to`` and the size of the buffer containing not-yet-yielded results. If the buffer is full, the iteration over the upstream is paused until a result is yielded from the buffer. (default: no concurrency)
            ordered (bool, optional): If ``concurrency`` > 1, whether to preserve the order of upstream elements or to yield them as soon as they are processed. (default: preserves upstream order)
            via ("thread" or "process", optional): If ``concurrency`` > 1, whether to apply ``to`` using processes or threads. (default: via threads)
        Returns:
            Stream[R]: A stream of transformed elements.
        """
        validate_concurrency(concurrency)
        validate_via(via)
        return MapStream(self, to, concurrency, ordered, via)

    def amap(
        self,
        to: Callable[[T], Coroutine[Any, Any, U]],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> "Stream[U]":
        """
        Applies the async ``to`` on upstream elements and yields the results.

        Args:
            to (Callable[[T], Coroutine[Any, Any, U]]): The async transformation to be applied to each element.
            concurrency (int, optional): Represents both the number of async tasks concurrently applying ``to`` and the size of the buffer containing not-yet-yielded results. If the buffer is full, the iteration over the upstream is paused until a result is yielded from the buffer. (default: no concurrency)
            ordered (bool, optional): If ``concurrency`` > 1, whether to preserve the order of upstream elements or to yield them as soon as they are processed. (default: preserves upstream order)
        Returns:
            Stream[R]: A stream of transformed elements.
        """
        validate_concurrency(concurrency)
        return AMapStream(self, to, concurrency, ordered)

    def observe(self, what: str = "elements") -> "Stream[T]":
        """
        Logs the progress of iteration over this stream.

        To avoid flooding, logs are emitted only when the number of yielded elements (or errors) reaches powers of 2.

        Args:
            what (str): A plural noun describing the yielded objects (e.g., "cats", "dogs").

        Returns:
            Stream[T]: A stream of upstream elements with progress logging during iteration.
        """
        return ObserveStream(self, what)

    def skip(self, until: Union[int, Callable[[T], Any]]) -> "Stream[T]":
        """
        Skips ``until`` elements (if ``int``) or skips until ``until(elem)`` becomes truthy.

        Args:
            until (Union[int, Callable[[T], Any]]):
                - ``int``: The number of elements to skip.
                - ``Callable[[T], Any]``: Skips elements until encountering one for which ``until(elem)`` is truthy (this element and all the subsequent ones will be yielded).
        Returns:
            Stream: A stream of the upstream elements remaining after skipping.
        """
        validate_count_or_callable(until, name="until")
        return SkipStream(self, until)

    def askip(
        self, until: Union[int, Callable[[T], Coroutine[Any, Any, Any]]]
    ) -> "Stream[T]":
        """
        Skips ``until`` elements (if ``int``) or skips until ``await until(elem)`` becomes truthy.

        Args:
            until (Union[int, Callable[[T], Coroutine[Any, Any, Any]]]):
                - ``int``: The number of elements to skip.
                - ``Callable[[T], Any]``: Skips elements until encountering one for which ``await until(elem)`` is truthy (this element and all the subsequent ones will be yielded).
        Returns:
            Stream: A stream of the upstream elements remaining after skipping.
        """
        validate_count_or_callable(until, name="until")
        return ASkipStream(self, until)

    def throttle(
        self,
        up_to: int,
        *,
        per: datetime.timedelta,
    ) -> "Stream[T]":
        """
        Limits the speed of iteration to yield at most ``up_to`` elements (or exceptions) ``per`` time interval.

        .. code-block:: python

            # limits the number of requests made to 50 per minute:
            from datetime import timedelta
            (
                Stream(urls)
                .throttle(50, per=timedelta(minutes=1))
                .map(requests.get, concurrency=4)
            )

        Args:
            up_to (int, optional): Maximum number of elements (or exceptions) that must be yielded within the given time interval.
            per (datetime.timedelta, optional): The time interval during which maximum ``up_to`` elements (or exceptions) will be yielded.

        Returns:
            Stream[T]: A stream yielding at most ``up_to`` upstream elements (or exceptions) ``per`` time interval.
        """
        validate_positive_count(up_to, name="up_to")
        validate_positive_interval(per, name="per")
        return ThrottleStream(self, up_to, per)

    def truncate(self, when: Union[int, Callable[[T], Any]]) -> "Stream[T]":
        """
        Stops iterations over this stream when ``when`` elements have been yielded (if ``int``) or when ``when(elem)`` becomes truthy.

        Args:
            until (Union[int, Callable[[T], Any]]):
                - ``int``: Stops the iteration after ``when`` elements have been yielded.
                - ``Callable[[T], Any]``: Stops the iteration when the first element for which ``when(elem)`` is truthy is encountered, that element will not be yielded.

        Returns:
            Stream[T]: A stream whose iteration will stop ``when`` condition is met.
        """
        validate_count_or_callable(when, name="when")
        return TruncateStream(self, when)

    def atruncate(
        self, when: Union[int, Callable[[T], Coroutine[Any, Any, Any]]]
    ) -> "Stream[T]":
        """
        Stops iterations over this stream when ``when`` elements have been yielded (if ``int``) or when ``await when(elem)`` becomes truthy.

        Args:
            until (Union[int, Callable[[T], Any]]):
                - ``int``: Stops the iteration after ``when`` elements have been yielded.
                - ``Callable[[T], Coroutine[Any, Any, Any]]``: Stops the iteration when the first element for which ``await when(elem)`` is truthy is encountered, that element will not be yielded.

        Returns:
            Stream[T]: A stream whose iteration will stop ``when`` condition is met.
        """
        validate_count_or_callable(when, name="when")
        return ATruncateStream(self, when)


class DownStream(Stream[U], Generic[T, U]):
    """
    Stream having an upstream.
    """

    __slots__ = ()

    def __init__(self, upstream: Stream[T]) -> None:
        self._upstream: Stream[T] = upstream

    def __deepcopy__(self, memo: Dict[int, Any]) -> "DownStream[T, U]":
        new = copy.copy(self)
        new._upstream = copy.deepcopy(self._upstream, memo)
        return new

    @property
    def source(
        self,
    ) -> Union[
        Iterable, Callable[[], Iterable], AsyncIterable, Callable[[], AsyncIterable]
    ]:
        return self._upstream.source

    @property
    def upstream(self) -> Stream[T]:
        """
        Returns:
            Stream: Parent stream.
        """
        return self._upstream


class CatchStream(DownStream[T, Union[T, U]]):
    __slots__ = ("_errors", "_when", "_replace", "_finally_raise")

    def __init__(
        self,
        upstream: Stream[T],
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        when: Optional[Callable[[Exception], Any]],
        replace: Optional[Callable[[Exception], U]],
        finally_raise: bool,
    ) -> None:
        super().__init__(upstream)
        self._errors = errors
        self._when = when
        self._replace = replace
        self._finally_raise = finally_raise

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_catch_stream(self)


class ACatchStream(DownStream[T, Union[T, U]]):
    __slots__ = ("_errors", "_when", "_replace", "_finally_raise")

    def __init__(
        self,
        upstream: Stream[T],
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        when: Optional[Callable[[Exception], Coroutine[Any, Any, Any]]],
        replace: Optional[Callable[[Exception], Coroutine[Any, Any, U]]],
        finally_raise: bool,
    ) -> None:
        super().__init__(upstream)
        self._errors = errors
        self._when = when
        self._replace = replace
        self._finally_raise = finally_raise

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_acatch_stream(self)


class DistinctStream(DownStream[T, T]):
    __slots__ = ("_by", "_consecutive")

    def __init__(
        self,
        upstream: Stream[T],
        by: Optional[Callable[[T], Any]],
        consecutive: bool,
    ) -> None:
        super().__init__(upstream)
        self._by = by
        self._consecutive = consecutive

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_distinct_stream(self)


class ADistinctStream(DownStream[T, T]):
    __slots__ = ("_by", "_consecutive")

    def __init__(
        self,
        upstream: Stream[T],
        by: Optional[Callable[[T], Coroutine[Any, Any, Any]]],
        consecutive: bool,
    ) -> None:
        super().__init__(upstream)
        self._by = by
        self._consecutive = consecutive

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_adistinct_stream(self)


class FilterStream(DownStream[T, T]):
    __slots__ = ("_where",)

    def __init__(self, upstream: Stream[T], where: Callable[[T], Any]) -> None:
        super().__init__(upstream)
        self._where = where

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_filter_stream(self)


class AFilterStream(DownStream[T, T]):
    __slots__ = ("_where",)

    def __init__(
        self, upstream: Stream[T], where: Callable[[T], Coroutine[Any, Any, Any]]
    ) -> None:
        super().__init__(upstream)
        self._where = where

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_afilter_stream(self)


class FlattenStream(DownStream[Iterable[T], T]):
    __slots__ = ("_concurrency",)

    def __init__(self, upstream: Stream[Iterable[T]], concurrency: int) -> None:
        super().__init__(upstream)
        self._concurrency = concurrency

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_flatten_stream(self)


class AFlattenStream(DownStream[AsyncIterable[T], T]):
    __slots__ = ("_concurrency",)

    def __init__(self, upstream: Stream[AsyncIterable[T]], concurrency: int) -> None:
        super().__init__(upstream)
        self._concurrency = concurrency

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_aflatten_stream(self)


class ForeachStream(DownStream[T, T]):
    __slots__ = ("_do", "_concurrency", "_ordered", "_via")

    def __init__(
        self,
        upstream: Stream[T],
        do: Callable[[T], Any],
        concurrency: int,
        ordered: bool,
        via: "Literal['thread', 'process']",
    ) -> None:
        super().__init__(upstream)
        self._do = do
        self._concurrency = concurrency
        self._ordered = ordered
        self._via = via

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_foreach_stream(self)


class AForeachStream(DownStream[T, T]):
    __slots__ = ("_do", "_concurrency", "_ordered")

    def __init__(
        self,
        upstream: Stream[T],
        do: Callable[[T], Coroutine],
        concurrency: int,
        ordered: bool,
    ) -> None:
        super().__init__(upstream)
        self._do = do
        self._concurrency = concurrency
        self._ordered = ordered

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_aforeach_stream(self)


class GroupStream(DownStream[T, List[T]]):
    __slots__ = ("_size", "_interval", "_by")

    def __init__(
        self,
        upstream: Stream[T],
        size: Optional[int],
        interval: Optional[datetime.timedelta],
        by: Optional[Callable[[T], Any]],
    ) -> None:
        super().__init__(upstream)
        self._size = size
        self._interval = interval
        self._by = by

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_group_stream(self)


class AGroupStream(DownStream[T, List[T]]):
    __slots__ = ("_size", "_interval", "_by")

    def __init__(
        self,
        upstream: Stream[T],
        size: Optional[int],
        interval: Optional[datetime.timedelta],
        by: Optional[Callable[[T], Coroutine[Any, Any, Any]]],
    ) -> None:
        super().__init__(upstream)
        self._size = size
        self._interval = interval
        self._by = by

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_agroup_stream(self)


class GroupbyStream(DownStream[T, Tuple[U, List[T]]]):
    __slots__ = ("_key", "_size", "_interval")

    def __init__(
        self,
        upstream: Stream[T],
        key: Callable[[T], U],
        size: Optional[int],
        interval: Optional[datetime.timedelta],
    ) -> None:
        super().__init__(upstream)
        self._key = key
        self._size = size
        self._interval = interval

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_groupby_stream(self)


class AGroupbyStream(DownStream[T, Tuple[U, List[T]]]):
    __slots__ = ("_key", "_size", "_interval")

    def __init__(
        self,
        upstream: Stream[T],
        key: Callable[[T], Coroutine[Any, Any, U]],
        size: Optional[int],
        interval: Optional[datetime.timedelta],
    ) -> None:
        super().__init__(upstream)
        self._key = key
        self._size = size
        self._interval = interval

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_agroupby_stream(self)


class MapStream(DownStream[T, U]):
    __slots__ = ("_to", "_concurrency", "_ordered", "_via")

    def __init__(
        self,
        upstream: Stream[T],
        to: Callable[[T], U],
        concurrency: int,
        ordered: bool,
        via: "Literal['thread', 'process']",
    ) -> None:
        super().__init__(upstream)
        self._to = to
        self._concurrency = concurrency
        self._ordered = ordered
        self._via = via

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_map_stream(self)


class AMapStream(DownStream[T, U]):
    __slots__ = ("_to", "_concurrency", "_ordered")

    def __init__(
        self,
        upstream: Stream[T],
        to: Callable[[T], Coroutine[Any, Any, U]],
        concurrency: int,
        ordered: bool,
    ) -> None:
        super().__init__(upstream)
        self._to = to
        self._concurrency = concurrency
        self._ordered = ordered

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_amap_stream(self)


class ObserveStream(DownStream[T, T]):
    __slots__ = ("_what",)

    def __init__(self, upstream: Stream[T], what: str) -> None:
        super().__init__(upstream)
        self._what = what

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_observe_stream(self)


class SkipStream(DownStream[T, T]):
    __slots__ = "_until"

    def __init__(
        self,
        upstream: Stream[T],
        until: Union[int, Callable[[T], Any]],
    ) -> None:
        super().__init__(upstream)
        self._until = until

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_skip_stream(self)


class ASkipStream(DownStream[T, T]):
    __slots__ = "_until"

    def __init__(
        self,
        upstream: Stream[T],
        until: Union[int, Callable[[T], Coroutine[Any, Any, Any]]],
    ) -> None:
        super().__init__(upstream)
        self._until = until

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_askip_stream(self)


class ThrottleStream(DownStream[T, T]):
    __slots__ = ("_up_to", "_per")

    def __init__(
        self,
        upstream: Stream[T],
        up_to: Optional[int],
        per: Optional[datetime.timedelta],
    ) -> None:
        super().__init__(upstream)
        self._up_to = up_to
        self._per = per

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_throttle_stream(self)


class TruncateStream(DownStream[T, T]):
    __slots__ = "_when"

    def __init__(
        self,
        upstream: Stream[T],
        when: Union[int, Callable[[T], Any]],
    ) -> None:
        super().__init__(upstream)
        self._when = when

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_truncate_stream(self)


class ATruncateStream(DownStream[T, T]):
    __slots__ = "_when"

    def __init__(
        self,
        upstream: Stream[T],
        when: Union[int, Callable[[T], Coroutine[Any, Any, Any]]],
    ) -> None:
        super().__init__(upstream)
        self._when = when

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_atruncate_stream(self)
