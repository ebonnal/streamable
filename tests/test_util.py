import asyncio
import unittest

from streamable._util._functiontools import sidify, star
from streamable._util._futuretools import FIFOOSFutureResultCollection, FutureResult


class TestUtil(unittest.TestCase):
    def test_sidify(self) -> None:
        def f(x: int) -> int:
            return x**2

        self.assertEqual(f(2), 4)
        self.assertEqual(sidify(f)(2), 2)

        # test decoration
        @sidify
        def g(x):
            return x**2

        self.assertEqual(g(2), 2)

    def test_star(self) -> None:
        self.assertListEqual(
            list(map(star(lambda i, n: i * n), enumerate(range(10)))),
            list(map(lambda x: x**2, range(10))),
        )

        @star
        def mul(a: int, b: int) -> int:
            return a * b

        self.assertListEqual(
            list(map(mul, enumerate(range(10)))),
            list(map(lambda x: x**2, range(10))),
        )

    def test_os_future_result_collection_anext(self):
        result = object()
        future_results = FIFOOSFutureResultCollection()
        future_results.add_future(FutureResult(result))
        self.assertEqual(
            asyncio.run(future_results.__anext__()),
            result,
        )
