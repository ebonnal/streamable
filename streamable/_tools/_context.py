import sys
from contextlib import contextmanager


@contextmanager
def noop_context_manager():
    yield


if sys.version_info >= (3, 10):
    from contextlib import aclosing
else:
    from streamable._tools._iter import AsyncClosable
    from contextlib import AbstractAsyncContextManager

    class aclosing(AbstractAsyncContextManager):
        def __init__(self, thing: AsyncClosable):
            self.thing = thing

        async def __aenter__(self):
            return self.thing

        async def __aexit__(self, *exc_info):
            await self.thing.aclose()
