import asyncio
from typing import AsyncIterable, AsyncIterator, List, Set, TypeVar

from streamable.util.asynctools import get_event_loop

T = TypeVar("T")


async def _aiter_to_list(aiterable: AsyncIterable[T]) -> List[T]:
    return [elem async for elem in aiterable]


def aiterable_to_list(aiterable: AsyncIterable[T]) -> List[T]:
    return get_event_loop().run_until_complete(_aiter_to_list(aiterable))


async def _aiter_to_set(aiterable: AsyncIterable[T]) -> Set[T]:
    return {elem async for elem in aiterable}


def aiterable_to_set(aiterable: AsyncIterable[T]) -> Set[T]:
    return get_event_loop().run_until_complete(_aiter_to_set(aiterable))


async def arange(start: int, end: int, step: int = 1) -> AsyncIterator[int]:
    for i in range(start, end, step):
        yield i
