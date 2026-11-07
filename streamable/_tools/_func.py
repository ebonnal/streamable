from functools import partial
from inspect import iscoroutinefunction
from typing import (
    Callable,
    Coroutine,
    Optional,
    TypeVar,
    Union,
    cast,
    overload,
)

from streamable._tools._async import AsyncFunction

T = TypeVar("T")
U = TypeVar("U")


def _sidified(func: Callable[[T], object], arg: T) -> T:
    func(arg)
    return arg


@overload
def sidify(
    func: AsyncFunction[T, object],
) -> AsyncFunction[T, T]: ...


@overload
def sidify(func: Callable[[T], object]) -> Callable[[T], T]: ...


def sidify(
    func: Callable[[T], object],
) -> Callable[[T], Union[T, Coroutine[object, object, T]]]:
    if iscoroutinefunction(func):

        async def wrap(arg: T) -> T:
            await cast(AsyncFunction[T, T], func)(arg)
            return arg

        return wrap
    return partial(_sidified, func)


async def _async_call(func: Callable[[T], U], o: T) -> U:
    return func(o)


@overload
def asyncify(func: AsyncFunction[T, U]) -> AsyncFunction[T, U]: ...


@overload
def asyncify(
    func: Union[Callable[[T], U], AsyncFunction[T, U]],
) -> AsyncFunction[T, U]: ...


@overload
def asyncify(func: None) -> None: ...


def asyncify(
    func: Union[None, Callable[[T], U], AsyncFunction[T, U]],
) -> Optional[AsyncFunction[T, U]]:
    if func is None or iscoroutinefunction(func):
        return cast(Optional[AsyncFunction[T, U]], func)
    return partial(_async_call, cast(Callable[[T], U], func))


def noop() -> None:
    pass


async def anoop() -> None:
    pass
