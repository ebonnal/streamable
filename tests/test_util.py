import unittest

from streamable.util.functiontools import sidify, star


class TestUtil(unittest.TestCase):
    def test_sidify(self) -> None:
        f = lambda x: x**2
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
