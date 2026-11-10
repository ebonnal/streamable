from dataclasses import dataclass
from typing import TypeVar, Union

T = TypeVar("T")


@dataclass
class ExceptionContainer:
    __slots__ = ("exception",)
    exception: BaseException

    class ClearedException(BaseException):
        __slots__ = ()
        pass

    @staticmethod
    def raise_if_instance(obj: Union[T, "ExceptionContainer"]) -> T:
        if isinstance(obj, ExceptionContainer):
            try:
                raise obj.exception
            finally:
                obj.exception = ExceptionContainer.ClearedException()
                del obj
        return obj
