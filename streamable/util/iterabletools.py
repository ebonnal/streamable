import asyncio
from typing import AsyncIterable, AsyncIterator, List, Set, TypeVar

T = TypeVar("T")


async def _aiter_to_list(aiterable: AsyncIterable[T]) -> List[T]:
    return [elem async for elem in aiterable]


def aiterable_to_list(aiterable: AsyncIterable[T]) -> List[T]:
    return asyncio.run(_aiter_to_list(aiterable))


async def _aiter_to_set(aiterable: AsyncIterable[T]) -> Set[T]:
    return {elem async for elem in aiterable}


def aiterable_to_set(aiterable: AsyncIterable[T]) -> Set[T]:
    return asyncio.run(_aiter_to_set(aiterable))


async def arange(start: int, end: int, step: int = 1) -> AsyncIterator[int]:
    for i in range(start, end, step):
        yield i
