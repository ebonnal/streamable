from concurrent.futures import Executor
from typing import AsyncContextManager, Type
import sys
from types import TracebackType
from typing import ContextManager, Optional, TypeVar

T = TypeVar("T")
E = TypeVar("E", bound=Executor)


if sys.version_info >= (3, 10):
    from contextlib import aclosing
else:  # pragma: no cover
    from streamable._tools._iter import AsyncClosable

    class aclosing(AsyncContextManager):
        def __init__(self, thing: AsyncClosable):
            self.thing = thing

        async def __aenter__(self):
            return self.thing

        async def __aexit__(self, *exc_info):
            await self.thing.aclose()


class NoopContextManager(ContextManager[T]):
    __slots__ = ("thing",)

    def __init__(self, thing: T):
        self.thing = thing

    def __enter__(self) -> T:
        return self.thing

    def __exit__(self, *exc_info) -> None:
        pass


class NoopAsyncContextManager(AsyncContextManager[T]):
    __slots__ = ("thing",)

    def __init__(self, thing: T):
        self.thing = thing

    async def __aenter__(self) -> T:
        return self.thing

    async def __aexit__(self, *exc_info) -> None:
        pass


class ExecutorAsyncContextManager(AsyncContextManager[E]):
    __slots__ = ("executor",)

    def __init__(self, executor: E):
        self.executor = executor

    async def __aenter__(self) -> E:
        return self.executor

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.executor.shutdown(
            wait=not exc or isinstance(exc, (Exception, GeneratorExit))
        )
