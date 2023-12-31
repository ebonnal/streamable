import unittest

from streamable._util import LimitedYieldsIteratorWrapper, sidify


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

    def test_controling_iterator_wrapper(self) -> None:
        l = []
        ciw = LimitedYieldsIteratorWrapper(
            map(lambda i: l.append(i), range(10)), initial_available_yields=1
        )
        next(ciw)
        self.assertListEqual(l, list(range(1)))
        self.assertEqual(ciw.available_yields, 0)
