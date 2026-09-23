import sys

if sys.version_info >= (3, 10):
    from contextlib import aclosing
else:
    from streamable._tools._iter import AsyncCloseable

    class aclosing:
        def __init__(self, thing: AsyncCloseable):
            self.thing = thing

        async def __aenter__(self):
            return self.thing

        async def __aexit__(self, *exc_info):
            await self.thing.aclose()
