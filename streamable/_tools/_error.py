from functools import partial
from typing import Callable, NamedTuple, TypeVar, Union

from streamable._tools._async import AsyncFunction

T = TypeVar("T")
U = TypeVar("U")


def contained(func: Callable[[T], U], arg: T) -> Union[U, "ExceptionContainer"]:
    try:
        return func(arg)
    except Exception as e:
        return ExceptionContainer(e)


async def acontained(
    afunc: AsyncFunction[T, U], arg: T
) -> Union[U, "ExceptionContainer"]:
    try:
        return await afunc(arg)
    except Exception as e:
        return ExceptionContainer(e)


class ExceptionContainer(NamedTuple):
    exception: Exception

    @staticmethod
    def wrap(func: Callable[[T], U]) -> Callable[[T], Union[U, "ExceptionContainer"]]:
        return partial(contained, func)

    @staticmethod
    def awrap(
        afunc: AsyncFunction[T, U],
    ) -> AsyncFunction[T, Union[U, "ExceptionContainer"]]:
        return partial(acontained, afunc)
