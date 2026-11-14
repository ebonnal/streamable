import asyncio
import sys
from typing import (
    AsyncIterator,
    Callable,
    Coroutine,
    Optional,
    TypeVar,
)

T = TypeVar("T")
U = TypeVar("U")

AsyncFunction = Callable[[T], Coroutine[object, object, U]]


def safe_get_event_loop() -> Optional[asyncio.AbstractEventLoop]:
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        pass
    if sys.version_info < (3, 14):  # pragma: no cover
        # check policy's `_local._loop` first to avoid
        # - `asyncio.get_event_loop`'s warning when no loop is set on 3.12/3.13
        # - `get_event_loop` setting a new loop when none is set on 3.8-3.11
        # - `get_event_loop`'s warning when a loop is already set on 3.8-3.11
        try:
            return asyncio.get_event_loop_policy()._local._loop  # type: ignore
        except AttributeError:
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


async def semaphored(
    into: AsyncFunction[T, U], elem: T, semaphore: asyncio.Semaphore
) -> U:
    async with semaphore:
        return await into(elem)
