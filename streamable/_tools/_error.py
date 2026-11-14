from typing import NamedTuple, TypeVar, Union

T = TypeVar("T")


class ExceptionContainer(NamedTuple):
    exception: BaseException

    @staticmethod
    def raise_if_instance(obj: Union[T, "ExceptionContainer"]) -> T:
        if isinstance(obj, ExceptionContainer):
            try:
                raise obj.exception
            finally:
                del obj
        return obj
