import asyncio
import sys
from typing import AsyncIterator, Awaitable, Callable, Coroutine, Optional, TypeVar

T = TypeVar("T")
R = TypeVar("R")

AsyncFunction = Callable[[T], Coroutine[object, object, R]]


def safe_get_event_loop() -> Optional[asyncio.AbstractEventLoop]:
    if (3, 12) <= sys.version_info < (3, 14):  # pragma: no cover
        # check policy's `_local._loop` first to avoid
        # `asyncio.get_event_loop`'s warning on 3.12/3.13 when no loop is set
        local = getattr(asyncio.get_event_loop_policy(), "_local", None)
        if local and not getattr(local, "_loop", None):
            return None
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        return None


# `builtins.anext` for pre 3.10
async def anext(aiterator: AsyncIterator[T], default: T) -> T:
    try:
        return await aiterator.__anext__()
    except StopAsyncIteration:
        return default


# before 3.12, `asyncio.wait_for` swallowed cancellations when the result was available.
if sys.version_info >= (3, 12):
    from asyncio import wait_for
else:  # pragma: no cover

    async def wait_for(awaitable: Awaitable[T], timeout: Optional[float]) -> T:
        if timeout is None:
            return await awaitable
        task = asyncio.ensure_future(awaitable)
        try:
            if timeout > 0:
                await asyncio.wait((task,), timeout=timeout)
        except BaseException:
            task.cancel()
            await asyncio.wait((task,))
            raise
        if not task.done():
            task.cancel()
            await asyncio.wait((task,))
            if task.cancelled():
                raise asyncio.TimeoutError
        return task.result()
