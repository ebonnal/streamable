from typing import Awaitable, TypeVar, Union

T = TypeVar("T")


class ExceptionContainer:
    __slots__ = ("exception",)

    def __init__(self, exception: BaseException) -> None:
        self.exception = exception

    class ClearedError(Exception):
        pass

    @staticmethod
    def raise_if_instance(obj: Union[T, "ExceptionContainer"]) -> T:
        if isinstance(obj, ExceptionContainer):
            try:
                raise obj.exception
            finally:
                obj.exception = ExceptionContainer.ClearedError()
        return obj

    @staticmethod
    async def acontaining(coro: Awaitable[T]) -> "Union[T, ExceptionContainer]":
        try:
            return await coro
        except BaseException as e:
            return ExceptionContainer(e)
