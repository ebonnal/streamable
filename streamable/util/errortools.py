from functools import partial
from typing import Any, Callable, Coroutine, NamedTuple, TypeVar, Union

T = TypeVar("T")
R = TypeVar("R")


class ErrorContainer(NamedTuple):
    error: Exception


def raise_if_error(elem: Union[T, ErrorContainer]) -> T:
    if isinstance(elem, ErrorContainer):
        raise elem.error
    return elem


def _containerize_errors(func: Callable[[T], R], arg: T) -> Union[R, ErrorContainer]:
    try:
        return func(arg)
    except Exception as e:
        return ErrorContainer(e)


def containerize_errors(
    func: Callable[[T], R],
) -> Callable[[T], Union[R, ErrorContainer]]:
    return partial(_containerize_errors, func)


async def acontainerize_errors(
    coro: Coroutine[Any, Any, R],
) -> Union[R, ErrorContainer]:
    try:
        return await coro
    except Exception as e:
        return ErrorContainer(e)
