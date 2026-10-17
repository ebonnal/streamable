from inspect import iscoroutinefunction
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Generic,
    Tuple,
    TypeVar,
    overload,
)

if TYPE_CHECKING:  # pragma: no cover
    from typing_extensions import TypeVarTuple, Unpack

    Ts = TypeVarTuple("Ts")

R = TypeVar("R")


class _Star(Generic[R]):
    __slots__ = ("func",)

    def __init__(self, func: Callable[..., R]) -> None:
        self.func = func

    def __call__(self, args: Tuple[object, ...]) -> R:
        return self.func(*args)

    def __repr__(self) -> str:
        return f"star({self.func.__name__})"


@overload
def star(
    func: "Callable[[Unpack[Ts]], Coroutine[object, object, R]]",
) -> "Callable[[Tuple[Unpack[Ts]]], Coroutine[object, object, R]]": ...


@overload
def star(func: "Callable[[Unpack[Ts]], R]") -> "Callable[[Tuple[Unpack[Ts]]], R]": ...


def star(func: "Callable[[Unpack[Ts]], Any]") -> "Callable[[Tuple[Unpack[Ts]]], Any]":
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

        async def astared(args: "Tuple[Unpack[Ts]]") -> Any:
            return await func(*args)

        astared.__name__ = f"star({func.__name__})"
        return astared
    return _Star(func)
