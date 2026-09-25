import sys
from typing import ContextManager, TypeVar

T = TypeVar("T")


if sys.version_info >= (3, 10):
    from contextlib import aclosing
else:  # pragma: no cover
    from streamable._tools._iter import AsyncClosable
    from contextlib import AbstractAsyncContextManager

    class aclosing(AbstractAsyncContextManager):
        def __init__(self, thing: AsyncClosable):
            self.thing = thing

        async def __aenter__(self):
            return self.thing

        async def __aexit__(self, *exc_info):
            await self.thing.aclose()


class NoopContextManager(ContextManager[T]):
    def __init__(self, thing: T):
        self.thing = thing

    def __enter__(self) -> T:
        return self.thing

    def __exit__(self, *exc_info) -> None:
        pass
