from inspect import iscoroutinefunction
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Tuple,
    TypeVar,
    cast,
)

if TYPE_CHECKING:  # pragma: no cover
    from typing_extensions import TypeVarTuple, Unpack

    Ts = TypeVarTuple("Ts")

U = TypeVar("U")


def func_name(func: Callable[..., Any]) -> str:
    return getattr(func, "__name__", repr(func))


class _Star(Generic[U]):
    __slots__ = ("func",)

    def __init__(self, func: Callable[..., U]) -> None:
        self.func = func

    def __call__(self, args: Tuple[object, ...]) -> U:
        return self.func(*args)

    def __repr__(self) -> str:
        return f"star({func_name(self.func)})"


def star(func: "Callable[[Unpack[Ts]], U]") -> "Callable[[Tuple[Unpack[Ts]]], U]":
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

        async def astarred(args: "Tuple[Unpack[Ts]]") -> Any:
            return await func(*args)

        astarred.__name__ = f"star({func_name(func)})"
        return cast("Callable[[Tuple[Unpack[Ts]]], U]", astarred)
    return _Star(func)
