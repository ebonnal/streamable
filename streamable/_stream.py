from concurrent.futures import Executor
import copy
import datetime
import logging
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
    NamedTuple,
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

from streamable._tools._async import AsyncFunction
from streamable._tools._iter import SyncAsyncIterable
from streamable._tools._logging import logfmt_str_escape, setup_logger
from streamable._tools._validation import (
    validate_concurrency_executor,
    validate_positive_timedelta,
    validate_int,
)
from streamable.visitors import Visitor
from streamable.visitors._iter import IteratorVisitor
from streamable.visitors._aiter import AsyncIteratorVisitor
from streamable.visitors._eq import EqualityVisitor
from streamable.visitors._repr import ReprVisitor
from streamable.visitors._repr import StrVisitor

# Initialize "streamable" logger
setup_logger()

if TYPE_CHECKING:  # pragma: no cover
    import builtins

    from typing_extensions import Concatenate, ParamSpec

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
        return self.accept(IteratorVisitor[T]())

    def __aiter__(self) -> AsyncIterator[T]:
        return self.accept(AsyncIteratorVisitor[T]())

    def __eq__(self, other: Any) -> bool:
        """
        Two streams are considered equal if they apply the same operations, with the same parameters, to the same source.

        Returns:
            ``bool``: True if this stream is equal to ``other``.
        """

        return self.accept(EqualityVisitor(other))

    def __repr__(self) -> str:
        return self.accept(ReprVisitor())

    def __str__(self) -> str:
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

    def cast(self, into: Type[U]) -> "stream[U]":
        """
        Casts the upstream elements.

        Args:
            into (``Type[U]``): The type to cast elements into.

        Returns:
            ``stream[U]``: A stream of casted upstream elements.
        """
        return cast(stream[U], self)

    @overload
    def catch(
        self,
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        *,
        where: Union[
            None, Callable[[Exception], Any], AsyncFunction[Exception, Any]
        ] = None,
        do: Union[
            None, Callable[[Exception], Any], AsyncFunction[Exception, Any]
        ] = None,
        replace: AsyncFunction[Exception, U],
        stop: bool = False,
    ) -> "stream[Union[T, U]]": ...

    @overload
    def catch(
        self,
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        *,
        where: Union[
            None, Callable[[Exception], Any], AsyncFunction[Exception, Any]
        ] = None,
        do: Union[
            None, Callable[[Exception], Any], AsyncFunction[Exception, Any]
        ] = None,
        replace: Callable[[Exception], U],
        stop: bool = False,
    ) -> "stream[Union[T, U]]": ...

    @overload
    def catch(
        self,
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        *,
        where: Union[
            None, Callable[[Exception], Any], AsyncFunction[Exception, Any]
        ] = None,
        do: Union[
            None, Callable[[Exception], Any], AsyncFunction[Exception, Any]
        ] = None,
        stop: bool = False,
    ) -> "stream[T]": ...

    def catch(
        self,
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        *,
        where: Union[
            None, Callable[[Exception], Any], AsyncFunction[Exception, Any]
        ] = None,
        do: Union[
            None, Callable[[Exception], Any], AsyncFunction[Exception, Any]
        ] = None,
        replace: Union[
            None, Callable[[Exception], U], AsyncFunction[Exception, U]
        ] = None,
        stop: bool = False,
    ) -> "stream[Union[T, U]]":
        """
        Catches the upstream exceptions if they are instances of ``errors`` type and they satisfy the ``where`` predicate.
        When an exception is caught you can ``do`` an effect and/or yield a replacement value ``replace(exc)``.

        The order of the calls is ``where`` -> ``do`` -> ``replace``.

        Args:
            errors (``Type[Exception] | Tuple[Type[Exception], ...]``): The exception types to catch.
            where (``Callable[[Exception], Any] | AsyncCallable[Exception, Any] | None``, optional): An additional condition that must be satisfied to catch the exception, i.e. ``where(exc)`` must be truthy. (default: no additional condition)
            do (``Callable[[Exception], Any] | AsyncCallable[Exception, Any] | None``, optional): Side effect to apply when an exception is caught (default: no side effect)
            replace (``Callable[[Exception], U] | AsyncCallable[Exception, U] | None``, optional): Replacement value yielded when an exception is caught. (default: do not yield any replacement value)
            stop (``bool``, optional): If True, catching an exception will stop the iteration. (default: iteration continues after an exception is caught)

        Returns:
            ``stream[T | U]``: A stream of upstream elements catching the eligible exceptions.
        """
        return CatchStream(
            self,
            errors,
            where=where,
            replace=replace,
            do=do,
            stop=stop,
        )

    def filter(
        self,
        where: Union[Callable[[T], Any], AsyncFunction[T, Any]] = bool,
    ) -> "stream[T]":
        """
        Filters the stream to yield only elements satisfying the ``where`` predicate.

        Args:
            where (``Callable[[T], Any] | AsyncCallable[T, Any]``, optional): An element is kept if ``where(elem)`` is truthy.

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
        self: "stream[str]",
        *,
        concurrency: int = 1,
    ) -> "stream[str]": ...

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
        effect: AsyncFunction[T, Any],
        *,
        concurrency: int = 1,
        as_completed: bool = False,
    ) -> "stream[T]": ...

    @overload
    def do(
        self,
        effect: Callable[[T], Any],
        *,
        concurrency: Union[int, Executor] = 1,
        as_completed: bool = False,
    ) -> "stream[T]": ...

    def do(
        self,
        effect: Union[Callable[[T], Any], AsyncFunction[T, Any]],
        *,
        concurrency: Union[int, Executor] = 1,
        as_completed: bool = False,
    ) -> "stream[T]":
        """
        Applies a side ``effect`` for each upstream element.

        Args:
            effect (``Callable[[T], Any] | AsyncCallable[T, Any]``): The function called on each upstream element as a side effect.
            concurrency (``int``, optional): The ``effect`` is applied ...

              - ``concurrency == 1`` (default): ... sequentially, no concurrency.
              - ``concurrency > 1`` or ``Executor``: ... concurrently via ``concurrency`` threads or via the provided ``Executor``, or via the event loop if ``effect`` is an async function. At any point in time, only ``concurrency`` elements are buffered for processing.

            as_completed (``bool``, optional): If ``concurrency`` > 1, whether to yield preserving the upstream order (First In First Out) or as completed (First Done First Out). (default: preserves order)

        Returns:
            ``stream[T]``: A stream of upstream elements, unchanged.
        """
        if isinstance(concurrency, int):
            validate_int(concurrency, gte=1, name="concurrency")
        else:
            validate_concurrency_executor(concurrency, effect, fn_name="effect")
        return DoStream(self, effect, concurrency, as_completed)

    @overload
    def group(
        self,
        up_to: Optional[int] = None,
        *,
        every: Optional[datetime.timedelta] = None,
        by: AsyncFunction[T, U],
    ) -> "stream[Tuple[U, List[T]]]": ...

    @overload
    def group(
        self,
        up_to: Optional[int] = None,
        *,
        every: Optional[datetime.timedelta] = None,
        by: Callable[[T], U],
    ) -> "stream[Tuple[U, List[T]]]": ...

    @overload
    def group(
        self,
        up_to: Optional[int] = None,
        *,
        every: Optional[datetime.timedelta] = None,
    ) -> "stream[List[T]]": ...

    def group(
        self,
        up_to: Optional[int] = None,
        *,
        every: Optional[datetime.timedelta] = None,
        by: Union[None, Callable[[T], U], AsyncFunction[T, U]] = None,
    ) -> "Union[stream[List[T]], stream[Tuple[U, List[T]]]]":
        """
        Groups upstream elements into lists.

        - if the group reaches ``up_to`` elements, it is yielded
        - if the ``every`` time interval elapsed since the last group was yielded, the group is yielded
        - if the upstream is exhausted, the group is yielded
        - if the upstream raises an exception, the group is yielded, and the exception is reraised when downstream requests the next group

        If ``by`` is specified, elements are accumulated in multiple groups, one per key, and ``(key, group)`` pairs are yielded.

        - if a group reaches ``up_to`` elements it is yielded
        - if the ``every`` time interval elapsed, the group containing the oldest element is yielded (i.e. FIFO)
        - if the upstream is exhausted, all groups are yielded (FIFO)
        - if the upstream raises an exception, all groups are yielded (FIFO), and the exception is reraised when downstream requests the next group

        Args:
            up_to (``int | None``, optional): The maximum size of the group. (default: no size limit)
            every (``datetime.timedelta | None``, optional): Yields a group if this time interval has elapsed since the last group was yielded. (default: no time limit)
            by (``Callable[[T], Any] | AsyncCallable[T, Any] | None``, optional): If specified, groups will only contain elements sharing the same ``by(elem)`` value, and ``(key, group)`` pairs are yielded. (default: does not co-group elements)
        Returns:
            ``stream[list[T]]``: A stream of upstream elements grouped into lists.
        """
        if up_to is not None:
            validate_int(up_to, gte=1, name="up_to")
        if every is not None:
            validate_positive_timedelta(every, name="every")
        return GroupStream(self, up_to, every, by)

    @overload
    def map(
        self,
        into: AsyncFunction[T, U],
        *,
        concurrency: int = 1,
        as_completed: bool = False,
    ) -> "stream[U]": ...

    @overload
    def map(
        self,
        into: Callable[[T], U],
        *,
        concurrency: Union[int, Executor] = 1,
        as_completed: bool = False,
    ) -> "stream[U]": ...

    def map(
        self,
        into: Union[Callable[[T], U], AsyncFunction[T, U]],
        *,
        concurrency: Union[int, Executor] = 1,
        as_completed: bool = False,
    ) -> "stream[U]":
        """
        Applies ``into`` on upstream elements and yields the results.

        Args:
            into (``Callable[[T], Any] | AsyncCallable[T, Any]``): The transformation applied to upstream elements.
            concurrency (``int``, optional): ``into`` is applied

              - ``concurrency == 1`` (default): ... sequentially, no concurrency.
              - ``concurrency > 1`` or ``Executor``: ... concurrently via ``concurrency`` threads or via the provided ``Executor``, or via the event loop if ``into`` is an async function. At any point in time, only ``concurrency`` elements are buffered for processing.

            as_completed (``bool``, optional): If ``concurrency`` > 1, whether to yield preserving the upstream order (First In First Out) or as completed (First Done First Out). (default: preserves order)

        Returns:
            ``stream[U]``: A stream of transformed elements.
        """
        if isinstance(concurrency, int):
            validate_int(concurrency, gte=1, name="concurrency")
        else:
            validate_concurrency_executor(concurrency, into, fn_name="into")
        return MapStream(self, into, concurrency, as_completed)

    class Observation(NamedTuple):
        """
        Represents the progress of iteration over a stream.
        Args:
            subject (``str``): Describes the stream's elements ("cats", "dogs", ...).
            elapsed (``datetime.timedelta``): The time elapsed since the iteration started.
            errors (``int``): The count of errors encountered.
            elements (``int``): The count of emitted elements.
        """

        subject: str
        elapsed: datetime.timedelta
        errors: int
        elements: int

        def __str__(self) -> str:
            """Returns a logfmt string representation of the observation."""
            subject = logfmt_str_escape(self.subject)
            elapsed = logfmt_str_escape(str(self.elapsed))
            return f"observed={subject} elapsed={elapsed} errors={self.errors} elements={self.elements}"

    def observe(
        self,
        subject: str = "elements",
        *,
        every: Union[None, int, datetime.timedelta] = None,
        do: Union[
            Callable[["stream.Observation"], Any],
            AsyncFunction["stream.Observation", Any],
        ] = logging.getLogger("streamable").info,
    ) -> "stream[T]":
        """
        Logs the progress of iteration over this stream: the time elapsed since the iteration started, the count of emitted elements and errors.

        A log is emitted `every` interval (number of elements or time interval), or when the number of yielded elements (or errors) reaches powers of 2 if `every is None`.

        Args:
            subject (``str``, optional): Describes the yielded objects ("cats", "dogs", ...). (default: "elements")
            every (``int | timedelta | None``, optional): When an upstream element is pulled, a log is emitted if ...

              - ``None`` (default): ... the number of yielded elements (or errors) reaches a power of 2.
              - ``int``: ... the number of yielded elements (or errors) reaches `every`.
              - ``timedelta``: ... `every` has elapsed since the last log.

            do (``Callable[[stream.Observation], Any] | AsyncCallable[stream.Observation, Any]``, optional): Specify what to do with the observation, ``do`` will be called periodically according to ``every``. A ``stream.Observation`` is passed to ``do``. (default: calls ``logging.getLogger("streamable").info``)

        Returns:
            ``stream[T]``: A stream of upstream elements with progress logging during iteration.
        """
        if isinstance(every, int):
            validate_int(every, gte=1, name="every")
        elif isinstance(every, datetime.timedelta):
            validate_positive_timedelta(every, name="every")
        return ObserveStream(self, subject, every, do)

    @overload
    def skip(self, until: int) -> "stream[T]": ...

    @overload
    def skip(
        self, *, until: Union[Callable[[T], Any], AsyncFunction[T, Any]]
    ) -> "stream[T]": ...

    def skip(
        self,
        until: Union[int, Callable[[T], Any], AsyncFunction[T, Any]],
    ) -> "stream[T]":
        """
        Skips ``until`` elements (if ``int``) or skips until ``until(elem)`` becomes truthy.

        Args:
            until (``int | Callable[[T], Any] | AsyncCallable[T, Any]``):

              - ``int``: The number of elements to skip.
              - ``Callable[[T], Any] | AsyncCallable[T, Any]``: Skips elements until encountering one for which ``until(elem)`` is truthy (this element and all the subsequent ones will be yielded).

        Returns:
            ``stream[T]``: A stream of the upstream elements remaining after skipping.
        """
        if isinstance(until, int):
            validate_int(until, gte=0, name="until")
        return SkipStream(self, until)

    @overload
    def take(self, until: int) -> "stream[T]": ...

    @overload
    def take(
        self, until: Union[Callable[[T], Any], AsyncFunction[T, Any]]
    ) -> "stream[T]": ...

    def take(
        self,
        until: Union[int, Callable[[T], Any], AsyncFunction[T, Any]],
    ) -> "stream[T]":
        """
        Yields the first ``until`` elements (if ``int``) or until ``until(elem)`` becomes truthy, and stop.

        Args:
            until (``int | Callable[[T], Any] | AsyncCallable[T, Any]``):

              - ``int``: Yields the first ``until`` elements.
              - ``Callable[[T], Any] | AsyncCallable[T, Any]``: Yields elements until encountering one for which ``until(elem)`` is truthy; that element will not be yielded.

        Returns:
            ``stream[T]``: A stream of the first upstream elements stopping according to ``until``.
        """
        if isinstance(until, int):
            validate_int(until, gte=0, name="until")
        return TakeStream(self, until)

    def throttle(
        self,
        up_to: int,
        *,
        per: datetime.timedelta,
    ) -> "stream[T]":
        """
        Limits the speed of iteration to at most ``up_to`` elements (or exceptions) ``per`` sliding time window.

        An element (or exception) is emitted if fewer than ``up_to`` emissions have been made during the last ``per`` time interval, or else it sleeps until the oldest emission leaves the window.


        Args:
            up_to (``int``): Maximum number of elements (or exceptions) allowed in the sliding time window.
            per (``datetime.timedelta``): The duration of the sliding time window.

        Returns:
            ``stream[T]``: A stream emitting at most ``up_to`` upstream elements (or exceptions) ``per`` sliding time window.
        """
        validate_int(up_to, gte=1, name="up_to")
        validate_positive_timedelta(per, name="per")
        return ThrottleStream(self, up_to, per)


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
    __slots__ = ("_errors", "_where", "_replace", "_do", "_stop")

    def __init__(
        self,
        upstream: stream[T],
        errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
        where: Union[
            None,
            Callable[[Exception], Any],
            AsyncFunction[Exception, Any],
        ],
        replace: Union[
            None,
            Callable[[Exception], U],
            AsyncFunction[Exception, U],
        ],
        do: Union[
            None,
            Callable[[Exception], Any],
            AsyncFunction[Exception, Any],
        ],
        stop: bool,
    ) -> None:
        super().__init__(upstream)
        self._errors = errors
        self._where = where
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
    __slots__ = ("_concurrency",)

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
    __slots__ = ("_effect", "_concurrency", "_as_completed")

    def __init__(
        self,
        upstream: stream[T],
        effect: Union[
            Callable[[T], Any],
            AsyncFunction[T, Any],
        ],
        concurrency: Union[int, Executor],
        as_completed: bool,
    ) -> None:
        super().__init__(upstream)
        self._effect = effect
        self._concurrency = concurrency
        self._as_completed = as_completed

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
            AsyncFunction[T, Any],
        ],
    ) -> None:
        super().__init__(upstream)
        self._up_to = up_to
        self._every = every
        self._by = by

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_group_stream(self)


class MapStream(DownStream[T, U]):
    __slots__ = ("_into", "_concurrency", "_as_completed")

    def __init__(
        self,
        upstream: stream[T],
        into: Union[
            Callable[[T], U],
            AsyncFunction[T, U],
        ],
        concurrency: Union[int, Executor],
        as_completed: bool,
    ) -> None:
        super().__init__(upstream)
        self._into = into
        self._concurrency = concurrency
        self._as_completed = as_completed

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_map_stream(self)


class ObserveStream(DownStream[T, T]):
    __slots__ = ("_subject", "_every", "_do")

    def __init__(
        self,
        upstream: stream[T],
        subject: str,
        every: Union[None, int, datetime.timedelta],
        do: Union[
            Callable[["stream.Observation"], Any],
            AsyncFunction["stream.Observation", Any],
        ],
    ) -> None:
        super().__init__(upstream)
        self._subject = subject
        self._every = every
        self._do = do

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_observe_stream(self)


class SkipStream(DownStream[T, T]):
    __slots__ = "_until"

    def __init__(
        self,
        upstream: stream[T],
        until: Union[
            int,
            Callable[[T], Any],
            AsyncFunction[T, Any],
        ],
    ) -> None:
        super().__init__(upstream)
        self._until = until

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_skip_stream(self)


class TakeStream(DownStream[T, T]):
    __slots__ = "_until"

    def __init__(
        self,
        upstream: stream[T],
        until: Union[
            int,
            Callable[[T], Any],
            AsyncFunction[T, Any],
        ],
    ) -> None:
        super().__init__(upstream)
        self._until = until

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_take_stream(self)


class ThrottleStream(DownStream[T, T]):
    __slots__ = ("_up_to", "_per")

    def __init__(
        self,
        upstream: stream[T],
        up_to: int,
        per: datetime.timedelta,
    ) -> None:
        super().__init__(upstream)
        self._up_to = up_to
        self._per = per

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_throttle_stream(self)
