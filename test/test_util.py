import unittest
from queue import Queue

from kioss.util import QueueIterator, sidify, has_next


class TestUtil(unittest.TestCase):
    def test_sidify(self):
        f = lambda x: x**2
        self.assertEqual(f(2), 4)
        self.assertEqual(sidify(f)(2), 2)

        @sidify
        def f(x):
            return x**2

        self.assertEqual(f(2), 2)

    def test_queue_iterator(self):
        queue = Queue()
        it = QueueIterator(queue, None)
        for i in range(8):
            queue.put(i)
        queue.put(None)
        self.assertListEqual(list(it), list(range(8)))

    def test_has_next(self):
        it = iter(range(3))
        self.assertTrue(has_next(it))
        self.assertEqual(next(it), 0)
        self.assertTrue(has_next(it))
        self.assertEqual(next(it), 1)
        self.assertTrue(has_next(it))
        self.assertEqual(next(it), 2)
        self.assertFalse(has_next(it))
        self.assertRaises(StopIteration, lambda: next(it))
