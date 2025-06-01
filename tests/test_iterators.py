import asyncio
import unittest
from typing import AsyncIterator

from streamable.aiterators import (
    _ConcurrentAMapAsyncIterable,
    _RaisingAsyncIterator,
)
from streamable.util.asynctools import awaitable_to_coroutine
from streamable.util.iterabletools import sync_to_async_iter
from tests.utils import async_identity, identity, src


class TestIterators(unittest.TestCase):
    def test_ConcurrentAMapAsyncIterable(self) -> None:
        with self.assertRaisesRegex(
            TypeError,
            r"(object int can't be used in 'await' expression)|('int' object can't be awaited)",
            msg="`amap` should raise a TypeError if a non async function is passed to it.",
        ):
            concurrent_amap_async_iterable: _ConcurrentAMapAsyncIterable[int, int] = (
                _ConcurrentAMapAsyncIterable(
                    sync_to_async_iter(iter(src)),
                    async_identity,
                    buffersize=2,
                    ordered=True,
                )
            )

            # remove error wrapping
            concurrent_amap_async_iterable.transformation = identity  # type: ignore

            aiterator: AsyncIterator[int] = _RaisingAsyncIterator(
                concurrent_amap_async_iterable.__aiter__()
            )
            print(
                asyncio.run(awaitable_to_coroutine(aiterator.__aiter__().__anext__()))
            )
