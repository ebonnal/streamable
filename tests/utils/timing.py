"""Helpers for timing test operations."""

import asyncio
import time
from typing import (
    Callable,
    Coroutine,
    Tuple,
    TypeVar,
    Union,
)


T = TypeVar("T")


async def time_coroutine(
    afn: Callable[[], Union[Coroutine[None, None, T], "asyncio.Future[T]"]],
    times: int = 1,
) -> Tuple[float, T]:
    """Time how long it takes to run an async function."""
    start = time.perf_counter()
    for _ in range(times):
        res = await afn()
    return (time.perf_counter() - start) / times, res
