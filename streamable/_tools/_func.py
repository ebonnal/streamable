from contextlib import suppress
from functools import partial
from typing import (
    TYPE_CHECKING,
    Callable,
    Coroutine,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

from streamable._tools._async import AsyncCallable, is_async_callable

T = TypeVar("T")
U = TypeVar("U")

if TYPE_CHECKING:
    from typing_extensions import ParamSpec

    P = ParamSpec("P")


def _sidified(func: Callable[[T], object], arg: T) -> T:
    func(arg)
    return arg


@overload
def sidify(
    func: AsyncCallable[T, object],
) -> AsyncCallable[T, T]: ...


@overload
def sidify(func: Callable[[T], object]) -> Callable[[T], T]: ...


def sidify(
    func: Union[
        Callable[[T], object], Callable[[T], Coroutine[object, object, object]]
    ],
) -> Callable[[T], Union[T, Coroutine[object, object, T]]]:
    if is_async_callable(func):

        async def wrap(arg: T) -> T:
            await func(arg)
            return arg

        return wrap
    return partial(_sidified, func)


async def _async_call(func: Callable[[T], U], o: T) -> U:
    return func(o)


@overload
def asyncify(func: AsyncCallable[T, U]) -> AsyncCallable[T, U]: ...


@overload
def asyncify(
    func: Union[Callable[[T], U], AsyncCallable[T, U]],
) -> AsyncCallable[T, U]: ...


@overload
def asyncify(func: None) -> None: ...


def asyncify(
    func: Union[None, Callable[[T], U], AsyncCallable[T, U]],
) -> Optional[AsyncCallable[T, U]]:
    if func is None or is_async_callable(func):
        return func
    return partial(_async_call, cast(Callable[[T], U], func))


def noop() -> None:
    pass


async def anoop() -> None:
    pass


def suppressing(
    error_type: Type[BaseException],
    func: "Callable[P, T]",
    *args: "P.args",
    **kwargs: "P.kwargs",
) -> Optional[T]:
    with suppress(error_type):
        return func(*args, **kwargs)
    return None
