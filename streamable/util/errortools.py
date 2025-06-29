from functools import partial
from typing import Any, Callable, Coroutine, NamedTuple, TypeVar, Union

T = TypeVar("T")
R = TypeVar("R")


class ErrorContainer(NamedTuple):
    error: Exception


def _containerize_errors(
    func: Callable[[T], R], arg: T
) -> Union[R, ErrorContainer]:
    try:
        return func(arg)
    except Exception as e:
        return ErrorContainer(e)


def containerize_errors(
    func: Callable[[T], R],
) -> Callable[[T], Union[R, ErrorContainer]]:
    return partial(_containerize_errors, func)


def acontainerize_errors(
    coro: Callable[[T], Coroutine[Any, Any, R]],
) -> Callable[[T], Coroutine[Any, Any, Union[R, ErrorContainer]]]:
    async def wrap(arg: T):
        try:
            return await coro(arg)
        except Exception as e:
            return ErrorContainer(e)

    return wrap
