from concurrent.futures import Executor
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
from streamable._tools._iter import (
    AsyncToSyncIterator,
    SyncAsyncIterable,
)
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
from streamable.visitors._check import InvolvesAsyncVisitor

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
    `stream[T]` wraps any `Iterable[T]` or `AsyncIterable[T]` with a lazy fluent interface covering concurrency, batching, buffering, rate limiting, progress logging, and error handling.

    Chain lazy operations, source elements are processed on-the-fly during iteration.

    Both sync and async functions are accepted by operations, you can freely mix them within the same `stream` and it can then be consumed as an `Iterable` or `AsyncIterable`. When a stream involving async functions is consumed as an `Iterable`, a temporary `asyncio` event loop is attached to it.

    Operations are implemented so that iteration can resume after caught exceptions.

    Args:
        source (``Iterable[T] | AsyncIterable[T] | Callable[[], T] | Callable[[], Coroutine[Any, Any, T]]``): Data source to wrap:

          ‣ ``Iterable[T] | AsyncIterable[T]``: any iterable (list, set, range, generator, etc...), or async iterable.
          ‣ ``Callable[[], T] | Callable[[], Coroutine[Any, Any, T]]``: sync or async function called sequentially to get the next element.

    Returns:
        ``stream[T]``: Stream instance wrapping the source.

    Example::

        import logging
        from datetime import timedelta
        from httpx import AsyncClient, Response, HTTPStatusError

        pokemons: stream[str] = (
            stream(range(10))
            .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
            .throttle(5, per=timedelta(seconds=1))
            .map(AsyncClient().get, concurrency=2)
            .do(Response.raise_for_status)
            .catch(HTTPStatusError, do=logging.warning)
            .map(lambda poke: poke.json()["name"])
        )

        >>> list(pokemons)
        ['bulbasaur', 'ivysaur', 'venusaur', ...]

        >>> [poke async for poke in pokemons]
        ['bulbasaur', 'ivysaur', 'venusaur', ...]
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
        The parent stream in the operation chain, if any.

        Returns:
            ``stream | None``
        """
        return self._upstream

    @property
    def source(self) -> Union[Iterable, AsyncIterable, Callable]:
        """
        The source of elements wrapped by this stream.

        Returns:
            ``Iterable | AsyncIterable | Callable``
        """
        return self._source

    def __iter__(self) -> Iterator[T]:
        if self.accept(InvolvesAsyncVisitor()):
            return AsyncToSyncIterator(self.__aiter__())
        return self.accept(IteratorVisitor[T]())

    def __aiter__(self) -> AsyncIterator[T]:
        return self.accept(AsyncIteratorVisitor[T]())

    def __eq__(self, other: Any) -> bool:
        """
        Check if this stream is equal to another stream.

        Two streams are equal if they apply the same operations with the same parameters to the same source.

        Args:
            other (``Any``): Object to compare with.

        Returns:
            ``bool``
        """

        return self.accept(EqualityVisitor(other))

    def __repr__(self) -> str:
        return self.accept(ReprVisitor())

    def __call__(self) -> "stream[T]":
        """
        Iterate until exhaustion without collecting elements.

        Returns:
            ``stream[T]``: This stream instance.

        Example::

            state: list[int] = []
            pipeline: stream[int] = stream(range(10)).do(state.append)
            pipeline()
            assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        """
        for _ in self:
            pass
        return self

    def __await__(self) -> Generator[int, None, "stream[T]"]:
        """
        Iterate as ``AsyncIterable`` until exhaustion without collecting elements.

        Returns:
            ``stream[T]``: This stream instance.

        Example::

            state: list[int] = []
            pipeline: stream[int] = stream(range(10)).do(state.append)
            await pipeline
            assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        """

        async def consume():
            async for _ in self:
                pass

        yield from (consume().__await__())
        return self

    def __add__(
        self, other: "Union[Iterable[U], AsyncIterable[U]]"
    ) -> "stream[Union[T, U]]":
        """
        Concatenate a stream with an iterable.

        Args:
            other (``Iterable[U] | AsyncIterable[U]``): iterable to concatenate with this stream.

        Returns:
            ``stream[T | U]``: Stream of ``self``'s elements followed by ``other``'s.

        Example::

            assert list(stream(range(10)) + range(10)) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        """
        chain = cast("Iterable[stream[Union[T, U]]]", (self, other))
        return cast("stream[Union[T, U]]", stream(chain).flatten())

    def accept(self, visitor: "Visitor[V]") -> V:
        """
        Accept a visitor to traverse the stream's operation chain.

        Args:
            visitor (``Visitor[V]``): Visitor instance that will traverse the stream chain.

        Returns:
            ``V``: Result of the visitor's traversal.
        """
        return visitor.visit_stream(self)

    def pipe(
        self,
        fn: "Callable[Concatenate[stream[T], P], U]",
        *args: "P.args",
        **kwargs: "P.kwargs",
    ) -> U:
        """
        Apply a callable on the stream.

        Args:
            fn (``Callable[Concatenate[stream[T], P], U]``): Function to call with the stream as first argument, followed by ``*args`` and ``**kwargs``.

            *args: Positional arguments passed to ``fn``.

            **kwargs: Keyword arguments passed to ``fn``.

        Returns:
            ``U``: Result of ``fn(self, *args, **kwargs)``.

        Example::

            import polars as pl
            pokemons: stream[str] = ...
            pokemons.pipe(pl.DataFrame, schema=["name"]).write_csv("pokemons.csv")
        """
        return fn(self, *args, **kwargs)

    def cast(self, into: Type[U]) -> "stream[U]":
        """
        Cast the elements ``into`` a different type.

        This is for type checkers only and has no impact on the iteration.

        Args:
            into (``type[U]``): Target type for stream elements. Only affects static type checking.

        Returns:
            ``stream[U]``: Stream with elements casted.

        Example::

            docs: stream[Any] = stream(['{"foo": "bar"}', '{"foo": "baz"}']).map(json.loads)
            dicts: stream[dict[str, str]] = docs.cast(dict[str, str])
            # the stream remains the same, it's for type checkers only
            assert dicts is docs
        """
        return cast(stream[U], self)

    def buffer(
        self,
        up_to: int,
    ) -> "stream[T]":
        """
        Buffer upstream elements via a background task ``up_to`` a given buffer size.

        It decouples upstream production rate from downstream consumption rate.

        The background task is a thread during a sync iteration, and an async task during an async iteration.

        Args:
            up_to (``int``): The buffer size. Must be >= 0. When reached, upstream pulling pauses until an element is yielded out of the buffer.

        Returns:
            ``stream[T]``: Stream with buffering.

        Example::

            pulled: list[int] = []
            buffered_ints = iter(
                stream(range(10))
                .do(pulled.append)
                .buffer(5)
            )
            assert next(buffered_ints) == 0
            time.sleep(1e-3)
            assert pulled == [0, 1, 2, 3, 4, 5]
        """
        validate_int(up_to, gte=1, name="up_to")
        return BufferStream(self, up_to)

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
        Catch and handle exceptions raised upstream.

        An exception is caught if it is of the ``errors`` type(s) provided in and if it satisfies the ``where`` predicate.

        When an exception is caught: the ``do`` callback is called, followed by ``replace``, if provided.

        Args:
            errors (``type[Exception] | tuple[type[Exception], ...]``): Exception type(s) to catch.

            where (``Callable[[Exception], Any] | AsyncCallable[Exception, Any] | None``, optional): Only exceptions for which ``where(exc)`` is truthy are caught.

            do (``Callable[[Exception], Any] | AsyncCallable[Exception, Any] | None``, optional): ``do(exception)`` is called when an exception is caught.

            replace (``Callable[[Exception], U] | AsyncCallable[Exception, U] | None``, optional): ``replace(exception)`` is yielded when an exception is caught.

            stop (``bool``, optional): If ``True``, iteration stops when an exception is caught.

        Returns:
            ``stream[T | U]``: Stream with exception handling.

        Example::

            inverses: stream[float] = (
                stream(range(10))
                .map(lambda n: round(1 / n, 2))
                .catch(ZeroDivisionError)
            )

            assert list(inverses) == [1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]

            # `where` a predicate is satisfied
            domains = ["github.com", "foo.bar", "google.com"]

            resolvable_domains: stream[str] = (
                stream(domains)
                .do(lambda domain: httpx.get(f"https://{domain}"), concurrency=2)
                .catch(httpx.HTTPError, where=lambda e: "not known" in str(e))
            )

            assert list(resolvable_domains) == ["github.com", "google.com"]

            # `do` a side effect on catch
            errors: list[Exception] = []
            inverses: stream[float] = (
                stream(range(10))
                .map(lambda n: round(1 / n, 2))
                .catch(ZeroDivisionError, do=errors.append)
            )
            assert list(inverses) == [1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
            assert len(errors) == 1

            # `replace` with a value
            inverses: stream[float] = (
                stream(range(10))
                .map(lambda n: round(1 / n, 2))
                .catch(ZeroDivisionError, replace=lambda e: float("inf"))
            )

            assert list(inverses) == [float("inf"), 1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]

            # `stop=True` to stop the iteration if an exception is caught
            inverses: stream[float] = (
                stream(range(10))
                .map(lambda n: round(1 / n, 2))
                .catch(ZeroDivisionError, stop=True)
            )

            assert list(inverses) == []
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
        Filter elements ``where`` a predicate is truthy.

        Filter out falsy elements if no predicate provided.

        Args:
            where (``Callable[[T], Any] | AsyncCallable[T, Any]``, optional): Predicate function. An element is kept if ``where(elem)`` is truthy.

        Returns:
            ``stream[T]``: Stream of elements passing the predicate.

        Example::

            even_ints: stream[int] = stream(range(10)).filter(lambda n: n % 2 == 0)
            assert list(even_ints) == [0, 2, 4, 6, 8]
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
        self: "stream[Tuple[U, ...]]",
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
        Explode upstream elements (``Iterable`` or ``AsyncIterable``) into individual elements.

        Args:
            concurrency (``int``, optional): Concurrency control:

              ‣ ``1`` (default): Fully flatten each upstream iterable before moving on to the next one.
              ‣ ``int > 1``: Flattens ``concurrency`` upstream iterables concurrently, moving to the next one only when one of the current ones is exhausted. The concurrent flattening happens in a round-robin fashion: each of the ``concurrency`` iterables yields its next element downstream in turn. The concurrency happens via threads, or async tasks for async upstream iterables.

        Returns:
            ``stream[U]``: Stream of all elements from upstream iterables.

        Example::

            chars: stream[str] = stream(["hel", "lo!"]).flatten()
            assert list(chars) == ["h", "e", "l", "l", "o", "!"]

            chars: stream[str] = stream(["hel", "lo", "!"]).flatten(concurrency=2)
            assert list(chars) == ["h", "l", "e", "o", "l", "!"]
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
        Perform a side ``effect`` on each upstream element, yielding them unchanged.

        Concurrency:

          - Set the ``concurrency`` to apply the side effect concurrently.

          - Only ``concurrency`` upstream elements are pulled for processing; the next upstream element is pulled only when the side effect completes.

          - It preserves the upstream order by default, but you can set ``as_completed=True`` to yield elements as their side effects complete.

        Args:
            effect (``Callable[[T], Any] | AsyncCallable[T, Any]``): The side effect function.

            concurrency (``int | Executor``, optional): Concurrency control:

              ‣ ``1`` (default): Sequential processing.
              ‣ ``int > 1``: Concurrent via ``concurrency`` threads or async tasks if ``effect`` is async.
              ‣ ``Executor``: Uses the provided executor (e.g., a ``ProcessPoolExecutor``), the concurrency is the number of workers.

            as_completed (``bool``, optional): Processing order:

              ‣ ``False`` (default): Preserves upstream order.
              ‣ ``True``: Processes side effects as they complete.

        Returns:
            ``stream[T]``: A stream of upstream elements.

        Example::

            state: list[int] = []
            store_ints: stream[int] = stream(range(10)).do(state.append)
            assert list(store_ints) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
            assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
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
        within: Optional[datetime.timedelta] = None,
        by: AsyncFunction[T, U],
    ) -> "stream[Tuple[U, List[T]]]": ...

    @overload
    def group(
        self,
        up_to: Optional[int] = None,
        *,
        within: Optional[datetime.timedelta] = None,
        by: Callable[[T], U],
    ) -> "stream[Tuple[U, List[T]]]": ...

    @overload
    def group(
        self,
        up_to: Optional[int] = None,
        *,
        within: Optional[datetime.timedelta] = None,
    ) -> "stream[List[T]]": ...

    def group(
        self,
        up_to: Optional[int] = None,
        *,
        within: Optional[datetime.timedelta] = None,
        by: Union[None, Callable[[T], U], AsyncFunction[T, U]] = None,
    ) -> "Union[stream[List[T]], stream[Tuple[U, List[T]]]]":
        """
        Group elements into batches:
        - ``up_to`` a given batch size
        - ``within`` a given time interval
        - ``by`` a given key, yielding ``(key, elements)`` pairs

        You can combine these parameters.

        If an exception is encountered during grouping, the pending batch is yielded (all the pending batches if `by` is set), and the exception is then raised.

        Args:
            up_to (``int | None``, optional): If a batch reaches that number of elements, it is yielded.

            within (``timedelta | None``, optional): A batch pending for more than ``within`` is yielded, even if under ``up_to`` elements.

            by (``Callable[[T], U] | AsyncCallable[T, U] | None``, optional): Co-group elements into ``(key, elements)`` tuples.
        Returns:
            ``stream[list[T]]`` if ``by is None``, else ``stream[tuple[U, list[T]]]``.

        Example::

            int_batches: stream[list[int]] = stream(range(10)).group(5)

            assert list(int_batches) == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]

            # `within` a given time interval
            from datetime import timedelta
            int_1sec_batches: stream[list[int]] = (
                stream(range(10))
                .throttle(2, per=timedelta(seconds=1))
                .group(within=timedelta(seconds=0.99))
            )

            assert list(int_1sec_batches) == [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]]

            # `by` a given key, yielding `(key, elements)` pairs
            ints_by_parity: stream[tuple[str, list[int]]] = (
                stream(range(10))
                .group(by=lambda n: "odd" if n % 2 else "even")
            )

            assert list(ints_by_parity) == [("even", [0, 2, 4, 6, 8]), ("odd", [1, 3, 5, 7, 9])]
        """
        if up_to is not None:
            validate_int(up_to, gte=1, name="up_to")
        if within is not None:
            validate_positive_timedelta(within, name="within")
        return GroupStream(self, up_to, within, by)

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
        Transform upstream elements.

        Concurrency:

          - Set the ``concurrency`` to apply the transformation concurrently.

          - Only ``concurrency`` upstream elements are pulled for processing; the next upstream element is pulled only when a result is yielded downstream.

          - It preserves the upstream order by default, but you can set ``as_completed=True`` to yield results as they become available.

        Args:
            into (``Callable[[T], U] | AsyncCallable[T, U]``): The transformation function.

            concurrency (``int | Executor``, optional): Concurrency control:

              ‣ ``1`` (default): Sequential processing.
              ‣ ``int > 1``: Concurrent via ``concurrency`` threads or async tasks if ``into`` is async.
              ‣ ``Executor``: Uses the provided executor (e.g., a ``ProcessPoolExecutor``), the concurrency is the number of workers.

            as_completed (``bool``, optional): Results order:

              ‣ ``False`` (default): Preserves upstream order.
              ‣ ``True``: Yields results as they become available.

        Returns:
            ``stream[U]``: A stream of transformed elements.

        Example::

            int_chars: stream[str] = stream(range(10)).map(str)
            assert list(int_chars) == ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
        """
        if isinstance(concurrency, int):
            validate_int(concurrency, gte=1, name="concurrency")
        else:
            validate_concurrency_executor(concurrency, into, fn_name="into")
        return MapStream(self, into, concurrency, as_completed)

    class Observation(NamedTuple):
        """
        Representation of the progress of iteration over a stream.

        Args:
            subject (``str``): Human-readable description of stream elements (e.g., "cats", "dogs", "requests").

            elapsed (``timedelta``): Time elapsed since iteration started.

            errors (``int``): Number of errors encountered during iteration so far.

            elements (``int``): Number of elements emitted so far.
        """

        subject: str
        elapsed: datetime.timedelta
        errors: int
        elements: int

        def __str__(self) -> str:
            """
            Return a logfmt-formatted string representation.

            Returns:
                ``str``: Logfmt string with subject, elapsed, errors, and elements.
            """
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
        Observe the iteration progress:
        - elapsed time since the iteration started
        - number of elements yielded by upstream
        - number of errors raised by upstream

        A ``stream.Observation`` is passed to the ``do`` callback (the default emits a log), at a frequency defined by ``every``.

        The errors raised by `do` itself are ignored: they do not bubble up into the iterator and are not included in the errors count.

        Args:
            subject (``str``, optional): Description of elements being observed.

            every (``int | timedelta | None``, optional): When to emit observations:

              ‣ ``None`` (default): When the elements/errors counts reach powers of 2.
              ‣ ``int``: Periodically when ``every`` elements or errors have been emitted.
              ‣ ``timedelta``: Periodically ``every`` time interval.

            do (``Callable[[stream.Observation], Any] | AsyncCallable[stream.Observation, Any]``, optional): Callback receiving a ``stream.Observation`` (subject, elapsed, errors, elements).

        Returns:
            ``stream[T]``: Stream with progress observation enabled.

        Example::

            observed_ints: stream[int] = stream(range(10)).observe("ints")
            assert list(observed_ints) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

            # observe every 1k elements (or errors)
            observed_ints = stream(range(10)).observe("ints", every=1000)
            # observe every 5 seconds
            observed_ints = stream(range(10)).observe("ints", every=timedelta(seconds=5))

            observed_ints = stream(range(10)).observe("ints", do=other_logger.info)
            observed_ints = stream(range(10)).observe("ints", do=logs.append)
            observed_ints = stream(range(10)).observe("ints", do=print)
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
        Skip a given number of elements, or skip ``until`` a predicate is satisfied.

        Args:
            until (``int | Callable[[T], Any] | AsyncCallable[T, Any]``): Skip control:

              ‣ ``int``: Skip first ``until`` elements. Must be >= 0.
              ‣ ``Callable[[T], Any] | AsyncCallable[T, Any]``: Skip until ``until(elem)`` is truthy, then yield that element and all subsequent.

        Returns:
            ``stream[T]``: Stream of remaining elements after skipping.

        Example::

            ints_after_5: stream[int] = stream(range(10)).skip(5)
            assert list(ints_after_5) == [5, 6, 7, 8, 9]

            ints_after_5: stream[int] = stream(range(10)).skip(until=lambda n: n >= 5)
            assert list(ints_after_5) == [5, 6, 7, 8, 9]
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
        Take a given number of elements, or take ``until`` a predicate is satisfied, remaining upstream elements are not consumed.

        Args:
            until (``int | Callable[[T], Any] | AsyncCallable[T, Any]``): Stop control:

              ‣ ``int``: Take first ``until`` elements. Must be >= 0.
              ‣ ``Callable[[T], Any] | AsyncCallable[T, Any]``: Take until ``until(elem)`` is truthy, then stop. Matching element is not yielded.

        Returns:
            ``stream[T]``: Stream of taken elements only.

        Example::

            first_5_ints: stream[int] = stream(range(10)).take(5)
            assert list(first_5_ints) == [0, 1, 2, 3, 4]

            first_5_ints: stream[int] = stream(range(10)).take(until=lambda n: n == 5)
            assert list(first_5_ints) == [0, 1, 2, 3, 4]
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
        Limit to ``up_to`` emissions (elements or exceptions) ``per`` time window.

        For each new upstream element or exception, if fewer than ``up_to`` were emitted in the last ``per`` interval, emits it immediately; otherwise sleeps until oldest emission leaves the sliding window.

        Args:
            up_to (``int``): Maximum emissions allowed in the sliding window (``per``). Must be >= 1.

            per (``timedelta``): Duration of the sliding time window. Must be positive.

        Returns:
            ``stream[T]``: Stream with rate limiting.

        Example::

            from datetime import timedelta
            three_ints_per_second: stream[int] = stream(range(10)).throttle(3, per=timedelta(seconds=1))
            # collects 10 ints in 3 seconds
            assert list(three_ints_per_second) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        """
        validate_int(up_to, gte=1, name="up_to")
        validate_positive_timedelta(per, name="per")
        return ThrottleStream(self, up_to, per)


class DownStream(stream[U], Generic[T, U]):
    """
    A stream having an upstream.
    """

    __slots__ = ()

    def __init__(self, upstream: stream[T]) -> None:
        self._upstream: stream[T] = upstream

    @property
    def source(self) -> Union[Iterable, AsyncIterable, Callable]:
        return self._upstream.source

    @property
    def upstream(self) -> stream[T]:
        """
        The parent stream that this downstream stream operates on.

        Returns:
            ``stream[T]``: Upstream stream in the operation chain.
        """
        return self._upstream


class BufferStream(DownStream[T, T]):
    __slots__ = ("_up_to",)

    def __init__(
        self,
        upstream: stream[T],
        up_to: int,
    ) -> None:
        super().__init__(upstream)
        self._up_to = up_to

    def accept(self, visitor: "Visitor[V]") -> V:
        return visitor.visit_buffer_stream(self)


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
    __slots__ = ("_up_to", "_within", "_by")

    def __init__(
        self,
        upstream: stream[T],
        up_to: Optional[int],
        within: Optional[datetime.timedelta],
        by: Union[
            None,
            Callable[[T], Any],
            AsyncFunction[T, Any],
        ],
    ) -> None:
        super().__init__(upstream)
        self._up_to = up_to
        self._within = within
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
