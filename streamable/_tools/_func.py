from functools import partial
from inspect import iscoroutinefunction
from typing import (
    Any,
    Callable,
    Coroutine,
    Generic,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
    overload,
)

from streamable._tools._async import AsyncFunction

T = TypeVar("T")
R = TypeVar("R")


def _sidified(func: Callable[[T], Any], arg: T) -> T:
    func(arg)
    return arg


@overload
def sidify(
    func: AsyncFunction[T, Any],
) -> AsyncFunction[T, T]: ...


@overload
def sidify(func: Callable[[T], Any]) -> Callable[[T], T]: ...


def sidify(func: Callable[[T], Any]) -> Callable[[T], Union[T, Coroutine[Any, Any, T]]]:
    if iscoroutinefunction(func):

        async def wrap(arg: T) -> T:
            await cast(AsyncFunction[T, T], func)(arg)
            return arg

        return wrap
    return partial(_sidified, func)


class _Star(Generic[R]):
    __slots__ = ("func",)

    def __init__(self, func: Callable[..., R]) -> None:
        self.func = func

    def __call__(self, args: Tuple) -> R:
        return self.func(*args)

    def __repr__(self) -> str:
        return f"star({self.func.__name__})"


T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
T4 = TypeVar("T4")
T5 = TypeVar("T5")
T6 = TypeVar("T6")
T7 = TypeVar("T7")
T8 = TypeVar("T8")
T9 = TypeVar("T9")


# fmt: off
@overload
def star(func: Callable[[T], R]) -> Callable[[Tuple], R]: ...
@overload
def star(func: Callable[[T1, T2], R]) -> Callable[[Tuple[T1, T2]], R]: ...
@overload
def star(func: Callable[[T1, T2, T3], R]) -> Callable[[Tuple[T1, T2, T3]], R]: ...
@overload
def star(func: Callable[[T1, T2, T3, T4], R]) -> Callable[[Tuple[T1, T2, T3, T4]], R]: ...
@overload
def star(func: Callable[[T1, T2, T3, T4, T5], R]) -> Callable[[Tuple[T1, T2, T3, T4, T5]], R]: ...
@overload
def star(func: Callable[[T1, T2, T3, T4, T5, T6], R]) -> Callable[[Tuple[T1, T2, T3, T4, T5, T6]], R]: ...
@overload
def star(func: Callable[[T1, T2, T3, T4, T5, T6, T7], R]) -> Callable[[Tuple[T1, T2, T3, T4, T5, T6, T7]], R]: ...
@overload
def star(func: Callable[[T1, T2, T3, T4, T5, T6, T7, T8], R]) -> Callable[[Tuple[T1, T2, T3, T4, T5, T6, T7, T8]], R]: ...
@overload
def star(func: Callable[[T1, T2, T3, T4, T5, T6, T7, T8, T9], R]) -> Callable[[Tuple[T1, T2, T3, T4, T5, T6, T7, T8, T9]], R]: ...
@overload
def star(func: Callable[..., R]) -> Callable[[Tuple[Any, ...]], R]: ...
# fmt: on


def star(func: Callable[..., R]) -> Callable[[Tuple[Any, ...]], R]:
    """
    Transform a function (sync or async) that takes several positional arguments into a function that takes a tuple.

    .. code-block:: python

        @star
        def add(a: int, b: int) -> int:
            return a + b

        assert add((2, 5)) == 7

        assert star(lambda a, b: a + b)((2, 5)) == 7
    """
    if iscoroutinefunction(func):

        async def astared(args: Tuple[Any, ...]) -> R:
            return await func(*args)

        astared.__name__ = f"star({func.__name__})"
        return cast(Callable[[Tuple[Any, ...]], R], astared)
    return _Star(func)


async def _async_call(func: Callable[[T], R], o: T) -> R:
    return func(o)


@overload
def asyncify(
    func: AsyncFunction[T, R],
) -> AsyncFunction[T, R]: ...


@overload
def asyncify(func: Callable[[T], R]) -> AsyncFunction[T, R]: ...


@overload
def asyncify(func: None) -> None: ...


def asyncify(
    func: Union[None, Callable[[T], R], AsyncFunction[T, R]],
) -> Optional[AsyncFunction[T, R]]:
    if not func or iscoroutinefunction(func):
        return cast(Optional[AsyncFunction[T, R]], func)
    return partial(_async_call, cast(Callable[[T], R], func))
