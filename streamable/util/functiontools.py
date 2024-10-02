import functools
from typing import Any, Callable, Coroutine, Tuple, Type, TypeVar, cast

T = TypeVar("T")
R = TypeVar("R")


def _reraise_as(
    func: Callable[[T], R], source: Type[Exception], target: Type[Exception], arg: T
) -> R:
    try:
        return func(arg)
    except source as e:
        raise target() from e


# picklable
def reraise_as(
    func: Callable[[T], R], source: Type[Exception], target: Type[Exception]
) -> Callable[[T], R]:
    return cast(
        Callable[["T"], "R"], functools.partial(_reraise_as, func, source, target)
    )


def _sidify(func: Callable[[T], Any], arg: T):
    func(arg)
    return arg


# picklable
def sidify(func: Callable[[T], Any]) -> Callable[[T], T]:
    return functools.partial(_sidify, func)


def async_sidify(
    func: Callable[[T], Coroutine]
) -> Callable[[T], Coroutine[Any, Any, T]]:
    async def wrap(arg: T) -> T:
        coroutine = func(arg)
        if not isinstance(coroutine, Coroutine):
            raise TypeError(
                f"The function is expected to be an async function, i.e. it must be a function returning a Coroutine object, but returned a {type(coroutine)}."
            )
        await coroutine
        return arg

    return wrap


def _star(func: Callable[..., R], args: Tuple) -> R:
    return func(*args)


def star(func: Callable[..., R]) -> Callable[[Tuple], R]:
    return functools.partial(_star, func)
