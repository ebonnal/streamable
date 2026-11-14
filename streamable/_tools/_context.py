import sys
from typing import AsyncContextManager, TypeVar

T = TypeVar("T")


if sys.version_info >= (3, 10):
    from contextlib import aclosing
else:  # pragma: no cover
    from streamable._tools._iter import AsyncClosable

    class aclosing(AsyncContextManager):
        __slots__ = ("thing",)

        def __init__(self, thing: AsyncClosable):
            self.thing = thing

        async def __aenter__(self):
            return self.thing

        async def __aexit__(self, *exc_info):
            await self.thing.aclose()
