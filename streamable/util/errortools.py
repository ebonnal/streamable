from typing import Any, Callable, Coroutine, NamedTuple, TypeVar, Union

T = TypeVar("T")
R = TypeVar("R")


class ExceptionContainer(NamedTuple):
    exception: Exception


def containerize_errors(
    func: Callable[[T], R],
) -> Callable[[T], Union[R, ExceptionContainer]]:
    def wrap(arg: T):
        try:
            return func(arg)
        except Exception as e:
            return ExceptionContainer(e)

    return wrap


def acontainerize_errors(
    coro: Callable[[T], Coroutine[Any, Any, R]],
) -> Callable[[T], Coroutine[Any, Any, Union[R, ExceptionContainer]]]:
    async def wrap(arg: T):
        try:
            return await coro(arg)
        except Exception as e:
            return ExceptionContainer(e)

    return wrap
