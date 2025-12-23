import asyncio
from typing import AsyncIterator

import pytest

from streamable._aiterators import (
    _ConcurrentAMapAsyncIterable,
    _RaisingAsyncIterator,
)
from streamable._tools._async import awaitable_to_coroutine
from streamable._tools._iter import async_iter
from tests.utils.functions import async_identity, identity
from tests.utils.source import ints_src


def test_ConcurrentAMapAsyncIterable() -> None:
    # `amap` should raise a TypeError if a non coroutine function is passed to it.
    with pytest.raises(
        TypeError,
        match=r"(object int can't be used in 'await' expression)|('int' object can't be awaited)",
    ):
        concurrent_amap_async_iterable: _ConcurrentAMapAsyncIterable[int, int] = (
            _ConcurrentAMapAsyncIterable(
                async_iter(iter(ints_src)),
                async_identity,
                concurrency=2,
                ordered=True,
            )
        )

        # remove error wrapping
        concurrent_amap_async_iterable.to = identity  # type: ignore

        aiterator: AsyncIterator[int] = _RaisingAsyncIterator(
            concurrent_amap_async_iterable.__aiter__()
        )
        asyncio.run(awaitable_to_coroutine(aiterator.__aiter__().__anext__()))
