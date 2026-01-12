import asyncio
from typing import AsyncIterator

import pytest

from streamable._aiterators import _AsyncConcurrentMapAsyncIterable
from streamable._tools._async import awaitable_to_coroutine
from streamable._tools._error import RaisingAsyncIterator
from streamable._tools._iter import async_iter
from tests.utils.functions import async_identity, identity
from tests.utils.source import INTEGERS


def test_ConcurrentAMapAsyncIterable() -> None:
    # `amap` should raise a TypeError if a non coroutine function is passed to it.
    with pytest.raises(
        TypeError,
        match=r"(object int can't be used in 'await' expression)|('int' object can't be awaited)",
    ):
        concurrent_amap_async_iterable: _AsyncConcurrentMapAsyncIterable[int, int] = (
            _AsyncConcurrentMapAsyncIterable(
                async_iter(iter(INTEGERS)),
                async_identity,
                concurrency=2,
                as_completed=False,
            )
        )

        # remove error wrapping
        concurrent_amap_async_iterable.into = identity  # type: ignore

        aiterator: AsyncIterator[int] = RaisingAsyncIterator(
            concurrent_amap_async_iterable.__aiter__()
        )
        asyncio.run(awaitable_to_coroutine(aiterator.__aiter__().__anext__()))
