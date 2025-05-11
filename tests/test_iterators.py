import asyncio
import unittest
from typing import AsyncIterator

from streamable.aiterators import (
    _ConcurrentAMapAsyncIterable,
    _RaisingAsyncIterator,
)
from streamable.iterators import ObserveIterator, _ConcurrentMapIterable
from streamable.util.asynctools import awaitable_to_coroutine
from streamable.util.iterabletools import sync_to_async_iter
from tests.utils import async_identity, identity, src


class TestIterators(unittest.TestCase):
    def test_validation(self):
        with self.assertRaisesRegex(
            ValueError,
            "`buffersize` must be >= 1 but got 0",
            msg="`_ConcurrentMapIterable` constructor should raise for non-positive buffersize",
        ):
            _ConcurrentMapIterable(
                iterator=iter([]),
                transformation=str,
                concurrency=1,
                buffersize=0,
                ordered=True,
                via="thread",
            )

        with self.assertRaisesRegex(
            ValueError,
            "`base` must be > 0 but got 0",
            msg="",
        ):
            ObserveIterator(
                iterator=iter([]),
                what="",
                base=0,
            )

    def test_ConcurrentAMapAsyncIterable(self) -> None:
        with self.assertRaisesRegex(
            TypeError,
            r"must be an async function i\.e\. a function returning a Coroutine but it returned a <class 'int'>",
            msg="`amap` should raise a TypeError if a non async function is passed to it.",
        ):
            concurrent_amap_async_iterable: _ConcurrentAMapAsyncIterable[int, int] = (
                _ConcurrentAMapAsyncIterable(
                    sync_to_async_iter(src),
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
