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


class stream(Iterable[T], AsyncIterable[T], Awaitable["stream[T]"]):
    """
    ``stream[T]`` enriches any ``Iterable[T]`` or ``AsyncIterable[T]`` with a small set of chainable lazy operations for elegant data manipulation, including thread/coroutine concurrency, batching, rate limiting, and error handling.

    A ``stream[T]`` is both an ``Iterable[T]`` and an `AsyncIterable[T]``: a convenient bridge between the sync and async worlds.

    init
    ^^^^

    Create a ``stream[T]`` decorating an ``Iterable[T]`` or ``AsyncIterable[T]`` source:

    .. code-block:: python

        ints: stream[int] = stream(range(10))

    operate
    ^^^^^^^

    Chain ***lazy*** operations (only evaluated during iteration), each returning a new ***immutable*** ``stream``:

    .. code-block:: python

        inverses: stream[float] = (
            ints
            .map(lambda n: round(1 / n, 2))
            .catch(ZeroDivisionError)
        )

    iterate
    ^^^^^^^

    A ``stream`` is ***both*** ``Iterable`` and ``AsyncIterable``:

    .. code-block:: python

        >>> list(inverses)
        [1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
        >>> [i for i in inverses]
        [1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
        >>> [i async for i in inverses]
        [1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]

    Elements are processed ***on-the-fly*** as the iteration advances.

    Args:
        source (``Iterable[T] | AsyncIterable[T] | Callable[[], T] | Callable[[], Coroutine[Any, Any, T]]``): The iterable to decorate. Can be specified as a function (sync or async) that will be called sequentially to get the next source element.
    """

    __slots__ = ("_source", "_upstream")

    # fmt: off
    @overload
    def __init__(self, source: Iterable[T]) -> None: ...
    @overload
    def __init__(self, source: AsyncIterable[T]) -> None: ...
    @overload
    def __init__(self, source: Callable[[], Coroutine[Any, Any, T]]) -> None: ...
    @overload
    def __init__(self, source: Callable[[], T]) -> None: ...
    # fmt: on

    def __init__(
        self,
        source: Union[
            Iterable[T],
            AsyncIterable[T],
            Callable[[], Coroutine[Any, Any, T]],
            Callable[[], T],
        ],
    ) -> None:
        self._source = source
        self._upstream: "Optional[stream]" = None

    @property
    def upstream(self) -> "Optional[stream]":
        """
        The parent stream if any.

        Returns:
            ``Stream | None``
        """
        return self._upstream

    @property
    def source(self) -> Union[Iterable, AsyncIterable, Callable]:
        """
        The source of the stream's elements

        Returns:
            ``Iterable | AsyncIterable | Callable``
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
        Two streams are considered equal if they apply the same operations, with the same parameters, to the same source.

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

    def __call__(self) -> "stream[T]":
        """
        Iterates over this stream until exhaustion.

        Returns:
            ``stream[T]``: self.
        """
        for _ in self:
            pass
        return self

    def __await__(self) -> Generator[int, None, "stream[T]"]:
        """
        Iterates over this stream until exhaustion.

        Returns:
            ``stream[T]``: self.
        """

        async def consume():
            async for _ in self:
                pass

        yield from (consume().__await__())
        return self

    def __add__(self, other: "stream[T]") -> "stream[T]":
        """
        ``a + b`` returns a stream yielding all elements of ``a``, followed by all elements of ``b``.
        """
        return cast(stream[T], stream((self, other)).flatten())

    def __iadd__(self, other: "stream[T]") -> "stream[T]":
        return self + other

    def accept(self, visitor: "Visitor[V]") -> V:
        """
        Entry point to visit the stream lineage.
        """
        return visitor.visit_stream(self)

    def pipe(
        self,
        func: "Callable[Concatenate[stream[T], P], U]",
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
        do: Optional[Callable[[Exception], Coroutine[Any, Any, Any]]] = None,
        replace: Optional[Callable[[Exception], Coroutine[Any, Any, U]]] = None,
        stop: bool = False,
    ) -> "stream[Union[T, U]]": ...

    @overload
    def catch(
        self,
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        *,
        when: Optional[Callable[[Exception], Any]] = None,
        do: Optional[Callable[[Exception], Any]] = None,
        replace: Optional[Callable[[Exception], U]] = None,
        stop: bool = False,
    ) -> "stream[Union[T, U]]": ...

    def catch(
        self,
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        *,
        when: Union[
            None,
            Callable[[Exception], Any],
            Callable[[Exception], Coroutine[Any, Any, Any]],
        ] = None,
        do: Union[
            None,
            Callable[[Exception], Any],
            Callable[[Exception], Coroutine[Any, Any, Any]],
        ] = None,
        replace: Union[
            None,
            Callable[[Exception], U],
            Callable[[Exception], Coroutine[Any, Any, U]],
        ] = None,
        stop: bool = False,
    ) -> "stream[Union[T, U]]":
        """
        Catches the upstream exceptions if they are instances of ``errors`` type and they satisfy the ``when`` predicate.
        Optionally ``replace`` the error by a value (returned by ``replace(error)``), or ``do`` an effect.

        Args:
            errors (``Type[Exception] | Tuple[Type[Exception], ...]``): The exception types to catch.
            when (``Callable[[Exception], Any] | Callable[[Exception], Coroutine[Any, Any, Any]] | None``, optional): An additional condition that must be satisfied to catch the exception, i.e. ``when(exception)`` (or ``await when(exception)``) must be truthy. (default: no additional condition)
            do (``Callable[[Exception], Any] | Callable[[Exception], Coroutine[Any, Any, Any]] | None``, optional): Side effect to apply when an exception is caught (default: no side effect)
            replace (``Callable[[Exception], U] | Callable[[Exception], Coroutine[Any, Any, U]] | None``, optional): Replacement value yielded when an exception is caught. (default: do not yield any replacement value)
            stop (``bool``, optional): If True, catching an exception will stop the iteration. (default: iteration continues after an exception is caught)

        Returns:
            ``stream[T | U]``: A stream of upstream elements catching the eligible exceptions.
        """
        return CatchStream(
            self,
            errors,
            when=when,
            replace=replace,
            do=do,
            stop=stop,
        )

    @overload
    def filter(self, where: Callable[[T], Any] = bool) -> "stream[T]": ...

    @overload
    def filter(self, where: Callable[[T], Coroutine[Any, Any, Any]]) -> "stream[T]": ...

    def filter(
        self,
        where: Union[
            Callable[[T], Any],
            Callable[[T], Coroutine[Any, Any, Any]],
        ] = bool,
    ) -> "stream[T]":
        """
        Filters the stream to yield only elements satisfying the ``where`` predicate.

        Args:
            where (``Callable[[T], Any] | Callable[[T], Coroutine[Any, Any, Any]]``, optional): An element is kept if ``where(elem)`` is truthy.

        Returns:
            ``stream[T]``: A stream of upstream elements satisfying the `where` predicate.
        """
        return FilterStream(self, where)

    @overload
    def flatten(
        self: "stream[Iterable[U]]",
        *,
        concurrency: int = 1,
    ) -> "stream[U]": ...

    @overload
    def flatten(
        self: "stream[AsyncIterable[U]]",
        *,
        concurrency: int = 1,
    ) -> "stream[U]": ...
    @overload
    def flatten(
        self: "stream[Iterator[U]]",
        *,
        concurrency: int = 1,
    ) -> "stream[U]": ...

    @overload
    def flatten(
        self: "stream[AsyncIterator[U]]",
        *,
        concurrency: int = 1,
    ) -> "stream[U]": ...

    @overload
    def flatten(
        self: "stream[stream[U]]",
        *,
        concurrency: int = 1,
    ) -> "stream[U]": ...

    @overload
    def flatten(
        self: "stream[Collection[U]]",
        *,
        concurrency: int = 1,
    ) -> "stream[U]": ...

    @overload
    def flatten(
        self: "stream[Sequence[U]]",
        *,
        concurrency: int = 1,
    ) -> "stream[U]": ...

    @overload
    def flatten(
        self: "stream[List[U]]",
        *,
        concurrency: int = 1,
    ) -> "stream[U]": ...

    @overload
    def flatten(
        self: "stream[Set[U]]",
        *,
        concurrency: int = 1,
    ) -> "stream[U]": ...
    @overload
    def flatten(
        self: "stream[builtins.map[U]]",
        *,
        concurrency: int = 1,
    ) -> "stream[U]": ...

    @overload
    def flatten(
        self: "stream[builtins.filter[U]]",
        *,
        concurrency: int = 1,
    ) -> "stream[U]": ...

    @overload
    def flatten(
        self: "stream[range]",
        *,
        concurrency: int = 1,
    ) -> "stream[int]": ...

    @overload
    def flatten(
        self: "stream[SyncAsyncIterable[U]]",
        *,
        concurrency: int = 1,
    ) -> "stream[U]": ...
    @overload
    def flatten(
        self: "stream[Union[Iterable[U], AsyncIterable[U]]]",
        *,
        concurrency: int = 1,
    ) -> "stream[U]": ...
    @overload
    def flatten(
        self: "stream[Union[Iterator[U], AsyncIterator[U]]]",
        *,
        concurrency: int = 1,
    ) -> "stream[U]": ...

    def flatten(
        self: "stream[Union[Iterable[U], AsyncIterable[U]]]",
        *,
        concurrency: int = 1,
    ) -> "stream[U]":
        """
        Yields the elements of upstream elements assumed to be iterables (`Iterable` or `AsyncIterable`).

        Args:
            concurrency (``int``, optional): Number of upstream iterables concurrently flattened. (default: no concurrency)
        Returns:
            ``stream[R]``: A stream of flattened elements from upstream iterables.
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
    ) -> "stream[T]": ...

    @overload
    def do(
        self,
        effect: Callable[[T], Any],
        *,
        concurrency: Union[int, Executor] = 1,
        ordered: bool = True,
    ) -> "stream[T]": ...

    def do(
        self,
        effect: Union[
            Callable[[T], Any],
            Callable[[T], Coroutine[Any, Any, Any]],
        ],
        *,
        concurrency: Union[int, Executor] = 1,
        ordered: bool = True,
    ) -> "stream[T]":
        """
        Applies a side ``effect`` for each upstream element.

        Args:
            effect (``Callable[[T], Any] | Callable[[T], Coroutine[Any, Any, Any]]``): The function called on each upstream element as a side effect.
            concurrency (``int``, optional): The ``effect`` is applied ...

                - ``concurrency == 1`` (default): ... sequentially, no concurrency.
                - ``concurrency > 1`` or ``Executor``: ... concurrently via ``concurrency`` threads or via the provided ``Executor``, or via the event loop if ``effect`` is a coroutine function. At any point in time, only ``concurrency`` elements are buffered for processing.

            ordered (``bool``, optional): If ``concurrency`` > 1, whether to yield preserving the upstream order (First In First Out) or as completed (First Done First Out). (default: preserves order)

        Returns:
            ``stream[T]``: A stream of upstream elements, unchanged.
        """
        if isinstance(concurrency, int):
            validate_int(concurrency, gte=1, name="concurrency")
        else:
            validate_concurrency_executor(concurrency, effect, fn_name="effect")
        return DoStream(self, effect, concurrency, ordered)

    @overload
    def group(
        self,
        up_to: Optional[int] = None,
        *,
        every: Optional[datetime.timedelta] = None,
        by: Callable[[T], Coroutine[Any, Any, Any]],
    ) -> "stream[List[T]]": ...

    @overload
    def group(
        self,
        up_to: Optional[int] = None,
        *,
        every: Optional[datetime.timedelta] = None,
        by: Optional[Callable[[T], Any]] = None,
    ) -> "stream[List[T]]": ...

    def group(
        self,
        up_to: Optional[int] = None,
        *,
        every: Optional[datetime.timedelta] = None,
        by: Union[
            None,
            Callable[[T], Any],
            Callable[[T], Coroutine[Any, Any, Any]],
        ] = None,
    ) -> "stream[List[T]]":
        """
        Groups upstream elements into lists.

        A group is yielded when any of the following conditions is met:

        - it contains ``up_to`` elements
        - the ``every`` time interval elapsed since the last group was yielded
        - the upstream is exhausted.

        If ``by`` is specified, groups will only contain elements sharing the same ``by(elem)`` value (see ``.groupby`` for ``(key, elements)`` pairs).

        Args:
            up_to (``int | None``, optional): The maximum size of the group. (default: no size limit)
            every (``datetime.timedelta | None``, optional): Yields a group if this time interval has elapsed since the last group was yielded. (default: no time limit)
            by (``Callable[[T], Any] | Callable[[T], Coroutine[Any, Any, Any]] | None``, optional): If specified, groups will only contain elements sharing the same ``by(elem)`` value. (default: does not co-group elements)
        Returns:
            ``stream[list[T]]``: A stream of upstream elements grouped into lists.
        """
        if up_to is not None:
            validate_int(up_to, gte=1, name="up_to")
        if every is not None:
            validate_positive_timedelta(every, name="every")
        return GroupStream(self, up_to, every, by)

    @overload
    def groupby(
        self,
        by: Callable[[T], Coroutine[Any, Any, U]],
        *,
        up_to: Optional[int] = None,
        every: Optional[datetime.timedelta] = None,
    ) -> "stream[Tuple[U, List[T]]]": ...

    @overload
    def groupby(
        self,
        by: Callable[[T], U],
        *,
        up_to: Optional[int] = None,
        every: Optional[datetime.timedelta] = None,
    ) -> "stream[Tuple[U, List[T]]]": ...

    def groupby(
        self,
        by: Union[
            Callable[[T], U],
            Callable[[T], Coroutine[Any, Any, U]],
        ],
        *,
        up_to: Optional[int] = None,
        every: Optional[datetime.timedelta] = None,
    ) -> "stream[Tuple[U, List[T]]]":
        """
        Groups upstream elements into ``(key, elements)`` tuples.

        A group is yielded when any of the following conditions is met:

        - it contains ``up_to`` elements
        - the ``every`` time interval elapsed since the last group was yielded
        - the upstream is exhausted.

        Args:
            by (``Callable[[T], U] | Callable[[T], Coroutine[Any, Any, U]]``): A function that returns the group key for an element.
            up_to (``int | None``, optional): The maximum size of the group. (default: no size limit)
            every (``datetime.timedelta | None``, optional): Yields a group if this time interval has elapsed since the last group was yielded. (default: no time limit)

        Returns:
            ``stream[tuple[U, list[T]]]``: A stream of upstream elements grouped by key, as ``(key, elements)`` tuples.
        """
        if up_to is not None:
            validate_int(up_to, gte=1, name="up_to")
        if every is not None:
            validate_positive_timedelta(every, name="every")
        return GroupbyStream(self, by, up_to, every)

    @overload
    def map(
        self,
        into: Callable[[T], Coroutine[Any, Any, U]],
        *,
        concurrency: Union[int, Executor] = 1,
        ordered: bool = True,
    ) -> "stream[U]": ...

    @overload
    def map(
        self,
        into: Callable[[T], U],
        *,
        concurrency: Union[int, Executor] = 1,
        ordered: bool = True,
    ) -> "stream[U]": ...

    def map(
        self,
        into: Union[
            Callable[[T], U],
            Callable[[T], Coroutine[Any, Any, U]],
        ],
        *,
        concurrency: Union[int, Executor] = 1,
        ordered: bool = True,
    ) -> "stream[U]":
        """
        Applies ``into`` on upstream elements and yields the results.

        Args:
            into (``Callable[[T], Any] | Callable[[T], Coroutine[Any, Any, Any]]``): The transformation applied to upstream elements.
            concurrency (``int``, optional): ``into`` is applied

                - ``concurrency == 1`` (default): ... sequentially, no concurrency.
                - ``concurrency > 1`` or ``Executor``: ... concurrently via ``concurrency`` threads or via the provided ``Executor``, or via the event loop if ``into`` is a coroutine function. At any point in time, only ``concurrency`` elements are buffered for processing.

            ordered (``bool``, optional): If ``concurrency`` > 1, whether to yield preserving the upstream order (First In First Out) or as completed (First Done First Out). (default: preserves order)

        Returns:
            ``stream[U]``: A stream of transformed elements.
        """
        if isinstance(concurrency, int):
            validate_int(concurrency, gte=1, name="concurrency")
        else:
            validate_concurrency_executor(concurrency, into, fn_name="into")
        return MapStream(self, into, concurrency, ordered)

    def watch(
        self,
        label: str = "elements",
        *,
        every: Optional[Union[int, datetime.timedelta]] = None,
    ) -> "stream[T]":
        """
        Logs the progress of iteration over this stream: the duration since the iteration started, the count of yielded elements and errors.

        A log is emitted `every` interval (number of elements or time interval), or when the number of yielded elements (or errors) reaches powers of 2 if `every is None`.

        On reception of an upstream element (or exception), if a log should be emitted, it will be emitted before re-yielding the element (or re-raising the exception).

        Args:
            label (``str``): A plural noun describing the yielded objects (e.g., "cats", "dogs").
            every (``int | timedelta | None``): When an upstream element is pulled, a log is emitted if ...

              - ``None`` (default): ... the number of yielded elements (or errors) reaches a power of 2.
              - ``int``: ... the number of yielded elements (or errors) reaches `every`.
              - ``timedelta``: ... `every` has elapsed since the last log.

        Returns:
            ``stream[T]``: A stream of upstream elements with progress logging during iteration.
        """
        if isinstance(every, int):
            validate_int(every, gte=1, name="every")
        elif isinstance(every, datetime.timedelta):
            validate_positive_timedelta(every, name="every")
        return WatchStream(self, label, every)

    @overload
    def skip(
        self, *, until: Callable[[T], Coroutine[Any, Any, Any]]
    ) -> "stream[T]": ...

    @overload
    def skip(self, *, until: Callable[[T], Any]) -> "stream[T]": ...
    @overload
    def skip(self, until: int) -> "stream[T]": ...

    def skip(
        self,
        until: Union[
            int,
            Callable[[T], Any],
            Callable[[T], Coroutine[Any, Any, Any]],
        ],
    ) -> "stream[T]":
        """
        Skips ``until`` elements (if ``int``) or skips until ``until(elem)`` becomes truthy.

        Args:
            until (``int | Callable[[T], Any] | Callable[[T], Coroutine[Any, Any, Any]]``):

                - ``int``: The number of elements to skip.
                - ``Callable[[T], Any] | Callable[[T], Coroutine[Any, Any, Any]]``: Skips elements until encountering one for which ``until(elem)`` is truthy (this element and all the subsequent ones will be yielded).

        Returns:
            ``stream[T]``: A stream of the upstream elements remaining after skipping.
        """
        if isinstance(until, int):
            validate_int(until, gte=0, name="until")
        return SkipStream(self, until)

    def throttle(
        self,
        up_to: int,
        *,
        per: datetime.timedelta,
    ) -> "stream[T]":
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
            ``stream[T]``: A stream yielding at most ``up_to`` upstream elements (or exceptions) ``per`` time interval.
        """
        validate_int(up_to, gte=1, name="up_to")
        validate_positive_timedelta(per, name="per")
        return ThrottleStream(self, up_to, per)

    @overload
    def keep(
        self, *, until: Callable[[T], Coroutine[Any, Any, Any]]
    ) -> "stream[T]": ...

    @overload
    def keep(self, *, until: Callable[[T], Any]) -> "stream[T]": ...

    @overload
    def keep(self, until: int) -> "stream[T]": ...

    def keep(
        self,
        until: Union[
            int,
            Callable[[T], Any],
            Callable[[T], Coroutine[Any, Any, Any]],
        ],
    ) -> "stream[T]":
        """
        Yields the first ``until`` elements (if ``int``) or until ``until(elem)`` becomes truthy, and stop.

        Args:
            until (``int | Callable[[T], Any] | Callable[[T], Coroutine[Any, Any, Any]]``):

                - ``int``: Yields the first ``until`` elements.
                - ``Callable[[T], Any] | Callable[[T], Coroutine[Any, Any, Any]]``: Yields elements until encountering one for which ``until(elem)`` is truthy; that element will not be yielded.

        Returns:
            ``stream[T]``: A stream of the first upstream elements stopping according to ``until``.
        """
        if isinstance(until, int):
            validate_int(until, gte=0, name="until")
        return KeepStream(self, until)


class DownStream(stream[U], Generic[T, U]):
    """
    Stream having an upstream.
    """

    __slots__ = ()

    def __init__(self, upstream: stream[T]) -> None:
        self._upstream: stream[T] = upstream

    def __deepcopy__(self, memo: Dict[int, Any]) -> "DownStream[T, U]":
        new = copy.copy(self)
        new._upstream = copy.deepcopy(self._upstream, memo)
        return new

    @property
    def source(self) -> Union[Iterable, AsyncIterable, Callable]:
        return self._upstream.source

    @property
    def upstream(self) -> stream[T]:
        """
        Returns:
            ``Stream``: Parent stream.
        """
        return self._upstream


class CatchStream(DownStream[T, Union[T, U]]):
    __slots__ = ("_errors", "_when", "_replace", "_do", "_stop")

    def __init__(
        self,
        upstream: stream[T],
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
        do: Union[
            None,
            Callable[[Exception], Any],
            Callable[[Exception], Coroutine[Any, Any, Any]],
        ],
        stop: bool,
    ) -> None:
        super().__init__(upstream)
        self._errors = errors
        self._when = when
        self._replace = replace
        self._do = do
        self._stop = stop

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_catch_stream(self)


class FilterStream(DownStream[T, T]):
    __slots__ = ("_where",)

    def __init__(self, upstream: stream[T], where: Callable[[T], Any]) -> None:
        super().__init__(upstream)
        self._where = where

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_filter_stream(self)


class FlattenStream(DownStream[Union[Iterable[T], AsyncIterable[T]], T]):
    __slots__ = ("_concurrency", "_async")

    def __init__(
        self,
        upstream: stream[Union[Iterable[T], AsyncIterable[T]]],
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
        upstream: stream[T],
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
    __slots__ = ("_up_to", "_every", "_by")

    def __init__(
        self,
        upstream: stream[T],
        up_to: Optional[int],
        every: Optional[datetime.timedelta],
        by: Union[
            None,
            Callable[[T], Any],
            Callable[[T], Coroutine[Any, Any, Any]],
        ],
    ) -> None:
        super().__init__(upstream)
        self._up_to = up_to
        self._every = every
        self._by = by

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_group_stream(self)


class GroupbyStream(DownStream[T, Tuple[U, List[T]]]):
    __slots__ = ("_by", "_up_to", "_every")

    def __init__(
        self,
        upstream: stream[T],
        by: Union[
            Callable[[T], U],
            Callable[[T], Coroutine[Any, Any, U]],
        ],
        up_to: Optional[int],
        every: Optional[datetime.timedelta],
    ) -> None:
        super().__init__(upstream)
        self._by = by
        self._up_to = up_to
        self._every = every

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_groupby_stream(self)


class MapStream(DownStream[T, U]):
    __slots__ = ("_into", "_concurrency", "_ordered")

    def __init__(
        self,
        upstream: stream[T],
        into: Union[
            Callable[[T], U],
            Callable[[T], Coroutine[Any, Any, U]],
        ],
        concurrency: Union[int, Executor],
        ordered: bool,
    ) -> None:
        super().__init__(upstream)
        self._into = into
        self._concurrency = concurrency
        self._ordered = ordered

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_map_stream(self)


class WatchStream(DownStream[T, T]):
    __slots__ = ("_label", "_every")

    def __init__(
        self,
        upstream: stream[T],
        label: str,
        every: Optional[Union[int, datetime.timedelta]],
    ) -> None:
        super().__init__(upstream)
        self._label = label
        self._every = every

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_watch_stream(self)


class SkipStream(DownStream[T, T]):
    __slots__ = "_until"

    def __init__(
        self,
        upstream: stream[T],
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
        upstream: stream[T],
        up_to: Optional[int],
        per: Optional[datetime.timedelta],
    ) -> None:
        super().__init__(upstream)
        self._up_to = up_to
        self._per = per

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_throttle_stream(self)


class KeepStream(DownStream[T, T]):
    __slots__ = "_when"

    def __init__(
        self,
        upstream: stream[T],
        when: Union[
            int,
            Callable[[T], Any],
            Callable[[T], Coroutine[Any, Any, Any]],
        ],
    ) -> None:
        super().__init__(upstream)
        self._when = when

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_keep_stream(self)
