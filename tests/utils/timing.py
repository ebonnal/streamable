"""Helpers for timing test operations."""

import asyncio
import time
import timeit
from typing import (
    Callable,
    Coroutine,
    Iterable,
    List,
    Tuple,
    TypeVar,
    Union,
)

from streamable._stream import stream
from tests.utils.iteration import IterableType, alist_or_list

T = TypeVar("T")


def timestream(
    stream: stream[T], times: int = 1, itype: IterableType = Iterable
) -> Tuple[float, List[T]]:
    """Time how long it takes to iterate a stream."""
    res: List[T] = []

    def iterate():
        nonlocal res
        res = alist_or_list(stream, itype=itype)

    return timeit.timeit(iterate, number=times) / times, res


async def timecoro(
    afn: Callable[[], Union[Coroutine[None, None, T], "asyncio.Future[T]"]],
    times: int = 1,
) -> Tuple[float, T]:
    """Time how long it takes to run an async function."""
    start = time.perf_counter()
    for _ in range(times):
        res = await afn()
    return (time.perf_counter() - start) / times, res
