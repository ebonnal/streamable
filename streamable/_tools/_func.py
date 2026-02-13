from functools import partial
from inspect import iscoroutinefunction
from typing import (
    Any,
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
