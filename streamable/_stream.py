from concurrent.futures import Executor
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

from streamable._utils._iter import SyncAsyncIterable
from streamable._utils._validation import (
    validate_concurrency_executor,
    validate_positive_timedelta,
    validate_int,
)

if TYPE_CHECKING:  # pragma: no cover
    import builtins

    from typing_extensions import Concatenate, ParamSpec

    from streamable.visitors import Visitor

    P = ParamSpec("P")


U = TypeVar("U")
T = TypeVar("T")
V = TypeVar("V")


class Stream(Iterable[T], AsyncIterable[T], Awaitable["Stream[T]"]):
    """
    A ``Stream[T]`` decorates an ``Iterable[T]`` or ``AsyncIterable[T]`` with a **fluent interface** enabling the chaining of lazy operations.

    Args:
        source (``Iterable[T] | Callable[[], Iterable[T]] | AsyncIterable[T] | Callable[[], AsyncIterable[T]]``): The iterable to decorate. Can be specified via a function that will be called each time an iteration is started over the stream (i.e. for each call to ``iter(stream)``/``aiter(stream)``).
    """

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
        self._source = source
        self._upstream: "Optional[Stream]" = None

    @property
    def upstream(self) -> "Optional[Stream]":
        """
        Returns:
            ``Stream | None``: Parent stream if any.
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
            ``Iterable | Callable[[], Iterable] | AsyncIterable | Callable[[], AsyncIterable]``: The source of the stream's elements.
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
            ``bool``: True if this stream is equal to ``other``.
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
            ``Stream[T]``: self.
        """
        for _ in self:
            pass
        return self

    def __await__(self) -> Generator[int, None, "Stream[T]"]:
        """
        Iterates over this stream until exhaustion.

        Returns:
            ``Stream[T]``: self.
        """

        async def consume():
            async for _ in self:
                pass

        yield from (consume().__await__())
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

    def pipe(
        self,
        func: "Callable[Concatenate[Stream[T], P], U]",
        *args: "P.args",
        **kwargs: "P.kwargs",
    ) -> U:
        """
        Calls ``func``, with this stream as the first positional argument, optionally followed by ``*args`` and ``**kwargs``.

        Args:
            func (``Callable[Concatenate[Stream[T], P], U]``): The function to apply.
            *args (optional): Passed to ``func``.
            **kwargs (optional): Passed to ``func``.

        Returns:
            ``U``: Result of ``func(self, *args, **kwargs)``.
        """
        return func(self, *args, **kwargs)

    @overload
    def catch(
        self,
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        *,
        when: Optional[Callable[[Exception], Coroutine[Any, Any, Any]]] = None,
        replace: Optional[Callable[[Exception], Coroutine[Any, Any, U]]] = None,
        finally_raise: bool = False,
    ) -> "Stream[Union[T, U]]": ...

    @overload
    def catch(
        self,
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        *,
        when: Optional[Callable[[Exception], Any]] = None,
        replace: Optional[Callable[[Exception], U]] = None,
        finally_raise: bool = False,
    ) -> "Stream[Union[T, U]]": ...

    def catch(
        self,
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        *,
        when: Union[
            None,
            Callable[[Exception], Any],
            Callable[[Exception], Coroutine[Any, Any, Any]],
        ] = None,
        replace: Union[
            None,
            Callable[[Exception], U],
            Callable[[Exception], Coroutine[Any, Any, U]],
        ] = None,
        finally_raise: bool = False,
    ) -> "Stream[Union[T, U]]":
        """
        Catches the upstream exceptions if they are instances of ``errors`` type and they satisfy the ``when`` predicate.
        Optionally yields a replacement value (returned by ``replace(error)`` or ``await replace(error)``).
        If any exception was caught during the iteration and ``finally_raise=True``, the first exception caught will be raised when the iteration finishes.

        Args:
            errors (``Type[Exception] | Tuple[Type[Exception], ...]``): The exception types to catch.
            when (``Callable[[Exception], Any] | Callable[[Exception], Coroutine[Any, Any, Any]] | None``, optional): An additional condition that must be satisfied to catch the exception, i.e. ``when(exception)`` (or ``await when(exception)``) must be truthy. (default: no additional condition)
            replace (``Callable[[Exception], U] | Callable[[Exception], Coroutine[Any, Any, U]] | None``, optional): Replacement value yielded when an exception is caught. (default: do not yield any replacement value)
            finally_raise (``bool``, optional): If True the first exception caught is raised when upstream's iteration ends. (default: iteration ends without raising)

        Returns:
            ``Stream[T | U]``: A stream of upstream elements catching the eligible exceptions.
        """
        return CatchStream(
            self,
            errors,
            when=when,
            replace=replace,
            finally_raise=finally_raise,
        )

    @overload
    def distinct(
        self,
        by: Callable[[T], Coroutine[Any, Any, Any]],
        *,
        consecutive: bool = False,
    ) -> "Stream[T]": ...

    @overload
    def distinct(
        self,
        by: Optional[Callable[[T], Any]] = None,
        *,
        consecutive: bool = False,
    ) -> "Stream[T]": ...

    def distinct(
        self,
        by: Union[
            None,
            Callable[[T], Any],
            Callable[[T], Coroutine[Any, Any, Any]],
        ] = None,
        *,
        consecutive: bool = False,
    ) -> "Stream[T]":
        """
        Filters the stream to yield only distinct elements.
        If a deduplication ``by`` is specified, ``foo`` and ``bar`` are treated as duplicates when ``by(foo) == by(bar)`` (or ``await by(foo) == await by(bar)``).

        Among duplicates, the first encountered occurence in upstream order is yielded.

        Warning:
            During iteration, the distinct elements yielded are retained in memory to perform deduplication.
            Alternatively, remove only consecutive duplicates without memory footprint by setting ``consecutive=True``.

        Args:
            by (``Callable[[T], Any] | Callable[[T], Coroutine[Any, Any, Any]]``, optional):
                - ``Callable[[T], Any] | Callable[[T], Coroutine[Any, Any, Any]]``: Elements are deduplicated based on ``by(elem)``.
                - ``None``: The deduplication is performed on the elements themselves. (default)
            consecutive (``bool``, optional): Removes only consecutive duplicates if ``True``, or deduplicates globally if ``False``. (default: global deduplication)

        Returns:
            ``Stream[T]``: A stream containing only unique upstream elements.
        """
        return DistinctStream(self, by, consecutive)

    @overload
    def filter(self, where: Callable[[T], Any] = bool) -> "Stream[T]": ...

    @overload
    def filter(self, where: Callable[[T], Coroutine[Any, Any, Any]]) -> "Stream[T]": ...

    def filter(
        self,
        where: Union[
            Callable[[T], Any],
            Callable[[T], Coroutine[Any, Any, Any]],
        ] = bool,
    ) -> "Stream[T]":
        """
        Filters the stream to yield only elements satisfying the ``where`` predicate.

        Args:
            where (``Callable[[T], Any] | Callable[[T], Coroutine[Any, Any, Any]]``, optional): An element is kept if ``where(elem)`` is truthy.

        Returns:
            ``Stream[T]``: A stream of upstream elements satisfying the `where` predicate.
        """
        return FilterStream(self, where)

    # fmt: off
    @overload
    def flatten(
        self: "Stream[Iterable[U]]",
        *,
        concurrency: int = 1,
    ) -> "Stream[U]": ...

    @overload
    def flatten(
        self: "Stream[AsyncIterable[U]]",
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
        self: "Stream[AsyncIterator[U]]",
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
        self: "Stream[Collection[U]]",
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
        self: "Stream[List[U]]",
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
        self: "Stream[range]",
        *,
        concurrency: int = 1,
    ) -> "Stream[int]": ...

    @overload
    def flatten(
        self: "Stream[SyncAsyncIterable[U]]",
        *,
        concurrency: int = 1,
    ) -> "Stream[U]": ...
    @overload
    def flatten(
        self: "Stream[Union[Iterable[U], AsyncIterable[U]]]",
        *,
        concurrency: int = 1,
    ) -> "Stream[U]": ...
    @overload
    def flatten(
        self: "Stream[Union[Iterator[U], AsyncIterator[U]]]",
        *,
        concurrency: int = 1,
    ) -> "Stream[U]": ...
    # fmt: on

    def flatten(
        self: "Stream[Union[Iterable[U], AsyncIterable[U]]]",
        *,
        concurrency: int = 1,
    ) -> "Stream[U]":
        """
        Iterates over upstream elements assumed to be iterables (sync or async), and individually yields their items.

        Args:
            concurrency (``int``, optional): Number of upstream iterables concurrently flattened. (default: no concurrency)
        Returns:
            ``Stream[R]``: A stream of flattened elements from upstream iterables.
        """
        validate_int(concurrency, gte=1, name="concurrency")
        return FlattenStream(self, concurrency)

    @overload
    def do(
        self,
        effect: Callable[[T], Coroutine[Any, Any, Any]],
        *,
        concurrency: Union[int, Executor] = 1,
        ordered: bool = True,
    ) -> "Stream[T]": ...

    @overload
    def do(
        self,
        effect: Callable[[T], Any],
        *,
        concurrency: Union[int, Executor] = 1,
        ordered: bool = True,
    ) -> "Stream[T]": ...

    def do(
        self,
        effect: Union[
            Callable[[T], Any],
            Callable[[T], Coroutine[Any, Any, Any]],
        ],
        *,
        concurrency: Union[int, Executor] = 1,
        ordered: bool = True,
    ) -> "Stream[T]":
        """
        Applies a side ``effect`` for each upstream element.

        Args:
            effect (``Callable[[T], Any] | Callable[[T], Coroutine[Any, Any, Any]]``): The function called on each upstream element as a side effect.
            concurrency (``int``, optional): (default: no concurrency)
                - ``concurrency == 1``: The ``effect`` is applied sequentially.
                - ``concurrency > 1`` or ``Executor``: The ``effect`` is applied concurrently via ``concurrency`` threads or via the provided ``Executor``, or via the event loop if ``effect`` is a coroutine function. At any point in time, only ``concurrency`` elements are buffered for processing.

            ordered (``bool``, optional): If ``concurrency`` > 1, whether to yield preserving the upstream order (First In First Out) or as completed (First Done First Out). (default: preserves order)

        Returns:
            ``Stream[T]``: A stream of upstream elements, unchanged.
        """
        if isinstance(concurrency, int):
            validate_int(concurrency, gte=1, name="concurrency")
        else:
            validate_concurrency_executor(concurrency, effect, fn_name="effect")
        return DoStream(self, effect, concurrency, ordered)

    @overload
    def group(
        self,
        size: Optional[int] = None,
        *,
        interval: Optional[datetime.timedelta] = None,
        by: Callable[[T], Coroutine[Any, Any, Any]],
    ) -> "Stream[List[T]]": ...

    @overload
    def group(
        self,
        size: Optional[int] = None,
        *,
        interval: Optional[datetime.timedelta] = None,
        by: Optional[Callable[[T], Any]] = None,
    ) -> "Stream[List[T]]": ...

    def group(
        self,
        size: Optional[int] = None,
        *,
        interval: Optional[datetime.timedelta] = None,
        by: Union[
            None,
            Callable[[T], Any],
            Callable[[T], Coroutine[Any, Any, Any]],
        ] = None,
    ) -> "Stream[List[T]]":
        """
        Groups upstream elements into lists.

        A group is yielded when any of the following conditions is met:

        - The group reaches ``size`` elements.
        - ``interval`` seconds have passed since the last group was yielded.
        - The upstream source is exhausted.

        If ``by`` is specified, groups will only contain elements sharing the same ``by(elem)`` value (or ``await by(elem)``) (see ``.groupby`` for ``(key, elements)`` pairs).

        Args:
            size (``int | None``, optional): The maximum number of elements per group. (default: no size limit)
            interval (``float``, optional): Yields a group if ``interval`` seconds have passed since the last group was yielded. (default: no interval limit)
            by (``Callable[[T], Any] | Callable[[T], Coroutine[Any, Any, Any]] | None``, optional): If specified, groups will only contain elements sharing the same ``by(elem)`` value. (default: does not co-group elements)
        Returns:
            ``Stream[List[T]]``: A stream of upstream elements grouped into lists.
        """
        if size is not None:
            validate_int(size, gte=1, name="size")
        if interval is not None:
            validate_positive_timedelta(interval, name="interval")
        return GroupStream(self, size, interval, by)

    @overload
    def groupby(
        self,
        key: Callable[[T], Coroutine[Any, Any, U]],
        *,
        size: Optional[int] = None,
        interval: Optional[datetime.timedelta] = None,
    ) -> "Stream[Tuple[U, List[T]]]": ...

    @overload
    def groupby(
        self,
        key: Callable[[T], U],
        *,
        size: Optional[int] = None,
        interval: Optional[datetime.timedelta] = None,
    ) -> "Stream[Tuple[U, List[T]]]": ...

    def groupby(
        self,
        key: Union[
            Callable[[T], U],
            Callable[[T], Coroutine[Any, Any, U]],
        ],
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
            key (``Callable[[T], U] | Callable[[T], Coroutine[Any, Any, U]]``): A function that returns the group key for an element.
            size (``int | None``, optional): The maximum number of elements per group. (default: no size limit)
            interval (``datetime.timedelta | None``, optional): If specified, yields a group if ``interval`` seconds have passed since the last group was yielded. (default: no interval limit)

        Returns:
            ``Stream[Tuple[U, List[T]]]``: A stream of upstream elements grouped by key, as ``(key, elements)`` tuples.
        """
        if size is not None:
            validate_int(size, gte=1, name="size")
        if interval is not None:
            validate_positive_timedelta(interval, name="interval")
        return GroupbyStream(self, key, size, interval)

    @overload
    def map(
        self,
        to: Callable[[T], Coroutine[Any, Any, U]],
        *,
        concurrency: Union[int, Executor] = 1,
        ordered: bool = True,
    ) -> "Stream[U]": ...

    @overload
    def map(
        self,
        to: Callable[[T], U],
        *,
        concurrency: Union[int, Executor] = 1,
        ordered: bool = True,
    ) -> "Stream[U]": ...

    def map(
        self,
        to: Union[
            Callable[[T], U],
            Callable[[T], Coroutine[Any, Any, U]],
        ],
        *,
        concurrency: Union[int, Executor] = 1,
        ordered: bool = True,
    ) -> "Stream[U]":
        """
        Applies ``to`` on upstream elements and yields the results.

        Args:
            to (``Callable[[T], Any] | Callable[[T], Coroutine[Any, Any, Any]]``): The transformation applied to upstream elements.
            concurrency (``int``, optional): (default: no concurrency)
                - ``concurrency == 1``: ``to`` is applied sequentially.
                - ``concurrency > 1`` or ``Executor``: ``to`` is applied concurrently via ``concurrency`` threads or via the provided ``Executor``, or via the event loop if ``to`` is a coroutine function. At any point in time, only ``concurrency`` elements are buffered for processing.

            ordered (``bool``, optional): If ``concurrency`` > 1, whether to yield preserving the upstream order (First In First Out) or as completed (First Done First Out). (default: preserves order)

        Returns:
            ``Stream[U]``: A stream of transformed elements.
        """
        if isinstance(concurrency, int):
            validate_int(concurrency, gte=1, name="concurrency")
        else:
            validate_concurrency_executor(concurrency, to, fn_name="to")
        return MapStream(self, to, concurrency, ordered)

    def observe(self, what: str = "elements") -> "Stream[T]":
        """
        Logs the progress of iteration over this stream.

        To avoid flooding, logs are emitted only when the number of yielded elements (or errors) reaches powers of 2.

        Args:
            what (``str``): A plural noun describing the yielded objects (e.g., "cats", "dogs").

        Returns:
            ``Stream[T]``: A stream of upstream elements with progress logging during iteration.
        """
        return ObserveStream(self, what)

    @overload
    def skip(
        self, *, until: Callable[[T], Coroutine[Any, Any, Any]]
    ) -> "Stream[T]": ...

    @overload
    def skip(self, *, until: Callable[[T], Any]) -> "Stream[T]": ...
    @overload
    def skip(self, until: int) -> "Stream[T]": ...

    def skip(
        self,
        until: Union[
            int,
            Callable[[T], Any],
            Callable[[T], Coroutine[Any, Any, Any]],
        ],
    ) -> "Stream[T]":
        """
        Skips ``until`` elements (if ``int``) or skips until ``until(elem)`` becomes truthy.

        Args:
            until (``int | Callable[[T], Any] | Callable[[T], Coroutine[Any, Any, Any]]``):
                - ``int``: The number of elements to skip.
                - ``Callable[[T], Any] | Callable[[T], Coroutine[Any, Any, Any]]``: Skips elements until encountering one for which ``until(elem)`` (or ``await until(elem)``) is truthy (this element and all the subsequent ones will be yielded).

        Returns:
            ``Stream[T]``: A stream of the upstream elements remaining after skipping.
        """
        if isinstance(until, int):
            validate_int(until, gte=0, name="until")
        return SkipStream(self, until)

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
            up_to (``int``, optional): Maximum number of elements (or exceptions) that must be yielded within the given time interval.
            per (``datetime.timedelta``, optional): The time interval during which maximum ``up_to`` elements (or exceptions) will be yielded.

        Returns:
            ``Stream[T]``: A stream yielding at most ``up_to`` upstream elements (or exceptions) ``per`` time interval.
        """
        validate_int(up_to, gte=1, name="up_to")
        validate_positive_timedelta(per, name="per")
        return ThrottleStream(self, up_to, per)

    @overload
    def truncate(
        self, *, when: Callable[[T], Coroutine[Any, Any, Any]]
    ) -> "Stream[T]": ...

    @overload
    def truncate(self, *, when: Callable[[T], Any]) -> "Stream[T]": ...

    @overload
    def truncate(self, when: int) -> "Stream[T]": ...

    def truncate(
        self,
        when: Union[
            int,
            Callable[[T], Any],
            Callable[[T], Coroutine[Any, Any, Any]],
        ],
    ) -> "Stream[T]":
        """
        Stops iterations over this stream when ``when`` elements have been yielded (if ``int``) or when ``when(elem)`` becomes truthy.

        Args:
            when (``int | Callable[[T], Any] | Callable[[T], Coroutine[Any, Any, Any]]``):
                - ``int``: Stops the iteration after ``when`` elements have been yielded.
                - ``Callable[[T], Any] | Callable[[T], Coroutine[Any, Any, Any]]``: Stops the iteration when the first element for which ``when(elem)`` (or ``await when(elem)``) is truthy is encountered, that element will not be yielded.

        Returns:
            ``Stream[T]``: A stream whose iteration will stop ``when`` condition is met.
        """
        if isinstance(when, int):
            validate_int(when, gte=0, name="when")
        return TruncateStream(self, when)


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
            ``Stream``: Parent stream.
        """
        return self._upstream


class CatchStream(DownStream[T, Union[T, U]]):
    __slots__ = ("_errors", "_when", "_replace", "_finally_raise")

    def __init__(
        self,
        upstream: Stream[T],
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        when: Union[
            None,
            Callable[[Exception], Any],
            Callable[[Exception], Coroutine[Any, Any, Any]],
        ],
        replace: Union[
            None,
            Callable[[Exception], U],
            Callable[[Exception], Coroutine[Any, Any, U]],
        ],
        finally_raise: bool,
    ) -> None:
        super().__init__(upstream)
        self._errors = errors
        self._when = when
        self._replace = replace
        self._finally_raise = finally_raise

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_catch_stream(self)


class DistinctStream(DownStream[T, T]):
    __slots__ = ("_by", "_consecutive")

    def __init__(
        self,
        upstream: Stream[T],
        by: Union[
            None,
            Callable[[T], Any],
            Callable[[T], Coroutine[Any, Any, Any]],
        ],
        consecutive: bool,
    ) -> None:
        super().__init__(upstream)
        self._by = by
        self._consecutive = consecutive

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_distinct_stream(self)


class FilterStream(DownStream[T, T]):
    __slots__ = ("_where",)

    def __init__(self, upstream: Stream[T], where: Callable[[T], Any]) -> None:
        super().__init__(upstream)
        self._where = where

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_filter_stream(self)


class FlattenStream(DownStream[Union[Iterable[T], AsyncIterable[T]], T]):
    __slots__ = ("_concurrency", "_async")

    def __init__(
        self,
        upstream: Stream[Union[Iterable[T], AsyncIterable[T]]],
        concurrency: int,
    ) -> None:
        super().__init__(upstream)
        self._concurrency = concurrency

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_flatten_stream(self)


class DoStream(DownStream[T, T]):
    __slots__ = ("_effect", "_concurrency", "_ordered")

    def __init__(
        self,
        upstream: Stream[T],
        effect: Union[
            Callable[[T], Any],
            Callable[[T], Coroutine[Any, Any, Any]],
        ],
        concurrency: Union[int, Executor],
        ordered: bool,
    ) -> None:
        super().__init__(upstream)
        self._effect = effect
        self._concurrency = concurrency
        self._ordered = ordered

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_do_stream(self)


class GroupStream(DownStream[T, List[T]]):
    __slots__ = ("_size", "_interval", "_by")

    def __init__(
        self,
        upstream: Stream[T],
        size: Optional[int],
        interval: Optional[datetime.timedelta],
        by: Union[
            None,
            Callable[[T], Any],
            Callable[[T], Coroutine[Any, Any, Any]],
        ],
    ) -> None:
        super().__init__(upstream)
        self._size = size
        self._interval = interval
        self._by = by

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_group_stream(self)


class GroupbyStream(DownStream[T, Tuple[U, List[T]]]):
    __slots__ = ("_key", "_size", "_interval")

    def __init__(
        self,
        upstream: Stream[T],
        key: Union[
            Callable[[T], U],
            Callable[[T], Coroutine[Any, Any, U]],
        ],
        size: Optional[int],
        interval: Optional[datetime.timedelta],
    ) -> None:
        super().__init__(upstream)
        self._key = key
        self._size = size
        self._interval = interval

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_groupby_stream(self)


class MapStream(DownStream[T, U]):
    __slots__ = ("_to", "_concurrency", "_ordered")

    def __init__(
        self,
        upstream: Stream[T],
        to: Union[
            Callable[[T], U],
            Callable[[T], Coroutine[Any, Any, U]],
        ],
        concurrency: Union[int, Executor],
        ordered: bool,
    ) -> None:
        super().__init__(upstream)
        self._to = to
        self._concurrency = concurrency
        self._ordered = ordered

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_map_stream(self)


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
        until: Union[
            int,
            Callable[[T], Any],
            Callable[[T], Coroutine[Any, Any, Any]],
        ],
    ) -> None:
        super().__init__(upstream)
        self._until = until

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_skip_stream(self)


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
        when: Union[
            int,
            Callable[[T], Any],
            Callable[[T], Coroutine[Any, Any, Any]],
        ],
    ) -> None:
        super().__init__(upstream)
        self._when = when

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_truncate_stream(self)
