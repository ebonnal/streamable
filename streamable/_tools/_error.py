from functools import partial
from typing import (
    Callable,
    Iterator,
    NamedTuple,
    TypeVar,
    Union,
)

from streamable._tools._async import AsyncFunction
from streamable._tools._iter import ClosableAsyncIteratorWithUpstream

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


class RaisingIterator(Iterator[T]):
    __slots__ = ("upstream",)

    def __init__(
        self,
        upstream: Iterator[Union[T, ExceptionContainer]],
    ) -> None:
        self.upstream = upstream

    def __next__(self) -> T:
        elem = self.upstream.__next__()
        if isinstance(elem, ExceptionContainer):
            try:
                raise elem.exception
            finally:
                del elem
        return elem


class RaisingAsyncIterator(
    ClosableAsyncIteratorWithUpstream[Union[T, ExceptionContainer], T]
):
    __slots__ = ()

    async def _anext(self) -> T:
        elem = await self.upstream.__anext__()
        if isinstance(elem, ExceptionContainer):
            try:
                raise elem.exception
            finally:
                del elem
        return elem
