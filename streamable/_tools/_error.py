from concurrent.futures import Future
from typing import Awaitable, NamedTuple, TypeVar, Union

T = TypeVar("T")


class BaseExceptionContainer(NamedTuple):
    exception: BaseException


class ExceptionContainer(BaseExceptionContainer):
    __slots__ = ()

    @classmethod
    async def aresult(cls, future: Awaitable[T]) -> Union[T, "ExceptionContainer"]:
        try:
            return await future
        except Exception as e:
            return cls(e)
        finally:
            del future

    @classmethod
    def result(cls, future: "Future[T]") -> Union[T, "ExceptionContainer"]:
        try:
            return future.result()
        except Exception as e:
            return cls(e)
        finally:
            del future
