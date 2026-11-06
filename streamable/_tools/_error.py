import asyncio
from concurrent import futures
from typing import Awaitable, NamedTuple, TypeVar, Union

T = TypeVar("T")


class ExceptionContainer(NamedTuple):
    exception: Exception

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
