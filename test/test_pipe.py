import time
import timeit
from typing import List
import unittest

from kioss import Pipe

TEN_MS = 0.01


def ten_millis_identity(x):
    time.sleep(TEN_MS)
    return x


class TestPipe(unittest.TestCase):
    def test_init(self):
        # from iterable
        self.assertListEqual(list(Pipe(range(8))), list(range(8)))
        # from iterator
        self.assertListEqual(list(Pipe(iter(range(8)))), list(range(8)))

    def test_chain(self):
        self.assertListEqual(
            list(
                Pipe(range(2))
                .chain(Pipe(range(2, 4)), Pipe(range(4, 6)))
                .chain(Pipe(range(6, 8)))
            ),
            list(range(8)),
        )

    def test_merge(self):
        self.assertSetEqual(
            set(
                Pipe(iter(range(2)))
                .merge(Pipe(range(2, 4)), Pipe(range(4, 6)))
                .merge(Pipe(range(6, 8)))
            ),
            set(range(8)),
        )
        # execution_time

    def test_map(self):
        func = lambda x: x**2
        # non threaded
        self.assertListEqual(list(Pipe(range(8)).map(func)), list(map(func, range(8))))
        # threaded
        self.assertListEqual(
            list(Pipe(range(8)).map(func, num_threads=2)), list(map(func, range(8)))
        )
        # non-threaded vs threaded execution time
        pipe = Pipe(range(8)).map(ten_millis_identity)
        self.assertAlmostEqual(pipe.time(), TEN_MS * 8, delta=0.3 * (TEN_MS * 8))
        num_threads = 2
        pipe = Pipe(range(8)).map(ten_millis_identity, num_threads=num_threads)
        self.assertAlmostEqual(
            pipe.time(),
            TEN_MS * 8 / num_threads,
            delta=0.3 * (TEN_MS * 8) / num_threads,
        )
        # sidify
        l: List[int] = []

        def func_with_side_effect(x):
            res = x**2
            l.append(res)
            return res

        args = range(8)
        self.assertListEqual(
            list(Pipe(args).map(func_with_side_effect, sidify=True)), list(args)
        )
        self.assertListEqual(l, list(map(func, args)))

    def test_explode(self):
        self.assertListEqual(
            list(Pipe(["Hello World", "Happy to be here :)"]).map(str.split).flatten()),
            ["Hello", "World", "Happy", "to", "be", "here", ":)"],
        )
        self.assertEqual(
            sum(
                Pipe([["1 2 3", "4 5 6"], ["7", "8 9 10"]])
                .flatten()
                .map(str.split)
                .flatten()
                .map(int)
            ),
            55,
        )

    def test_filter(self):
        self.assertListEqual(list(Pipe(range(8)).filter(lambda x: x % 2)), [1, 3, 5, 7])

        self.assertListEqual(list(Pipe(range(8)).filter(lambda _: False)), [])

    def test_batch(self):
        self.assertListEqual(
            list(Pipe(range(8)).batch(max_size=3)), [[0, 1, 2], [3, 4, 5], [6, 7]]
        )
        self.assertListEqual(
            list(Pipe(range(6)).batch(max_size=3)), [[0, 1, 2], [3, 4, 5]]
        )
        self.assertListEqual(
            list(Pipe(range(8)).batch(max_size=1)), list(map(lambda x: [x], range(8)))
        )
        self.assertListEqual(list(Pipe(range(8)).batch(max_size=8)), [list(range(8))])

    def test_slow(self):
        freq = 64
        pipe = Pipe(range(8)).map(ten_millis_identity).slow(freq)
        self.assertAlmostEqual(
            pipe.time(),
            1 / freq * 8,
            delta=0.3 * (1 / freq * 8),
        )

    def test_head(self):
        self.assertListEqual(list(Pipe(range(8)).head(3)), [0, 1, 2])
        # stops after the second element
        self.assertAlmostEqual(
            Pipe(range(8)).map(ten_millis_identity).head(2).time(),
            TEN_MS * 2,
            delta=0.3 * TEN_MS * 2,
        )

    def test_list(self):
        self.assertListEqual(Pipe(range(8)).collect(limit=6), list(range(6)))
        self.assertListEqual(Pipe(range(8)).collect(), list(range(8)))
        self.assertAlmostEqual(
            timeit.timeit(
                lambda: Pipe(range(8)).map(ten_millis_identity).collect(0),
                number=1,
            ),
            TEN_MS * 8,
            delta=0.3 * TEN_MS * 8,
        )

    def test_reduce(self):
        self.assertEqual(Pipe(range(8)).map(lambda _: 1).reduce(int.__add__, 0), 8)

    def test_timeit(self):
        new_pipe = lambda: Pipe(range(8)).slow(64)
        start_time = time.time()
        list(new_pipe())
        execution_time = time.time() - start_time
        self.assertAlmostEqual(
            execution_time, new_pipe().time(), delta=0.3 * execution_time
        )

    def test_catch(self):
        self.assertListEqual(
            list(Pipe(["1", "r", "2"]).map(int).catch().map(type)),
            [int, ValueError, int],
        )

    def test_log(self):
        list(
            Pipe(range(8))
            .map(lambda elem: ("_" if elem % 2 else "") + str(elem))
            .map(int)
            .catch()
            .log()
        )
