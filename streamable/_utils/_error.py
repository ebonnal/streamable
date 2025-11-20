from typing import Callable, NamedTuple, TypeVar, Union

T = TypeVar("T")
U = TypeVar("U")


class ExceptionContainer(NamedTuple):
    exception: Exception

    @staticmethod
    def wrap(func: Callable[[T], U]) -> Callable[[T], Union[U, "ExceptionContainer"]]:
        def error_wrapping(_: T) -> Union[U, "ExceptionContainer"]:
            try:
                return func(_)
            except Exception as e:
                return ExceptionContainer(e)

        return error_wrapping
