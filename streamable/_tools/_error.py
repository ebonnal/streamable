import asyncio
from concurrent import futures
from dataclasses import dataclass
from typing import Awaitable, TypeVar, Union

T = TypeVar("T")


@dataclass
class ExceptionContainer:
    exception: Exception

    class ClearedException(Exception):
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

    @classmethod
    async def aresult(
        cls, future: Union["Awaitable[T]", "futures.Future[T]"]
    ) -> Union[T, "ExceptionContainer"]:
        try:
            if isinstance(future, futures.Future):
                return await asyncio.wrap_future(future)
            return await future
        except Exception as e:
            return cls(e)
        finally:
            del future

    @classmethod
    def result(
        cls, future: "Union[asyncio.Future[T], futures.Future[T]]"
    ) -> Union[T, "ExceptionContainer"]:
        try:
            return future.result()
        except Exception as e:
            return cls(e)
        finally:
            del future
