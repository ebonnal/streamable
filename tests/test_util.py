from typing import TypeVar
import unittest

from streamable.util.functiontools import sidify, star
from streamable.util.typetools import make_generic


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

    def test_make_generic(self) -> None:
        T = TypeVar("T")

        class Foo:
            pass

        with self.assertRaises(TypeError):
            Foo[T]
        make_generic(Foo)
        Foo[T]
