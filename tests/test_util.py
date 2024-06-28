import unittest

from streamable.util import sidify


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
