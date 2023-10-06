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
