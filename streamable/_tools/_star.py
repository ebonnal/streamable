from inspect import iscoroutinefunction
from typing import (
    Any,
    Callable,
    Generic,
    Tuple,
    TypeVar,
    cast,
    overload,
)

T = TypeVar("T")
T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
T4 = TypeVar("T4")
T5 = TypeVar("T5")
T6 = TypeVar("T6")
T7 = TypeVar("T7")
T8 = TypeVar("T8")
T9 = TypeVar("T9")
R = TypeVar("R")


class _Star(Generic[R]):
    __slots__ = ("func",)

    def __init__(self, func: Callable[..., R]) -> None:
        self.func = func

    def __call__(self, args: Tuple) -> R:
        return self.func(*args)

    def __repr__(self) -> str:
        return f"star({self.func.__name__})"


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
