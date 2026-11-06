import sys
from concurrent.futures import Executor
from typing import AsyncContextManager, ContextManager, TypeVar

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


class SyncToAsyncContextManager(AsyncContextManager[T]):
    __slots__ = ("context_manager",)

    def __init__(self, context_manager: ContextManager[T]):
        self.context_manager = context_manager

    async def __aenter__(self) -> T:
        return self.context_manager.__enter__()

    async def __aexit__(self, *exc_info) -> None:
        self.context_manager.__exit__(*exc_info)
