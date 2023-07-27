from collections import Counter
from functools import reduce
import time
import timeit
from typing import List
import unittest

from kioss import Pipe
from kioss import util
from kioss.util import identity

TEN_MS = 0.01


def ten_millis_identity(x):
    time.sleep(TEN_MS)
    return x


class TestPipe(unittest.TestCase):
    def test_init(self):
        # from iterable
        self.assertListEqual(Pipe(range(8)).collect(), list(range(8)))
        # from iterator
        self.assertListEqual(Pipe(range(8)).collect(), list(range(8)))

    def test_chain(self):
        self.assertListEqual(
            list(
                Pipe(range(2))
                .chain(Pipe(range(2, 4)), Pipe(range(4, 6)))
                .chain(Pipe(range(6, 8)))
            ),
            list(range(8)),
        )

    def test_mix(self):
        N = 32
        single_pipe_iteration_duration = 0.5
        new_pipes = lambda: [
            Pipe(range(0, N, 3)).slow((N / 3) / single_pipe_iteration_duration),
            Pipe(range(1, N, 3)).slow((N / 3) / single_pipe_iteration_duration),
            Pipe(range(2, N, 3)).slow((N / 3) / single_pipe_iteration_duration),
        ]
        self.assertAlmostEqual(
            timeit.timeit(
                lambda: self.assertSetEqual(
                    set(Pipe[int]().mix(*new_pipes())),
                    set(range(N)),
                ),
                number=1,
            ),
            single_pipe_iteration_duration,
            delta=0.3 * single_pipe_iteration_duration,
        )
        # same perf if chaining mix methods or if mixing in a single one
        pipes = new_pipes()
        self.assertSetEqual(
            set(Pipe().mix(*new_pipes())),
            set(reduce(Pipe.mix, pipes[1:], Pipe(pipes[0]))),
        )
        pipes = new_pipes()
        self.assertAlmostEqual(
            Pipe().mix(*new_pipes()).time(),
            reduce(Pipe.mix, pipes[1:], Pipe(pipes[0])).time(),
            delta=0.05 * Pipe().mix(*new_pipes()).time(),
        )

    def test_add(self):
        self.assertListEqual(
            list(
                sum(
                    [
                        Pipe(range(0, 2)),
                        Pipe(range(2, 4)),
                        Pipe(range(4, 6)),
                        Pipe(range(6, 8)),
                    ],
                    start=Pipe([]),
                )
            ),
            list(range(8)),
        )

    def test_map(self):
        func = lambda x: x**2
        # non threaded
        self.assertListEqual(list(Pipe(range(8)).map(func)), list(map(func, range(8))))
        # threaded
        self.assertSetEqual(
            set(Pipe(range(8)).map(func, n_threads=2)), set(map(func, range(8)))
        )
        # non-threaded vs threaded execution time
        N = 100
        pipe = Pipe(range(N)).map(ten_millis_identity)
        self.assertAlmostEqual(pipe.time(), TEN_MS * N, delta=0.3 * (TEN_MS * N))
        n_threads = 2
        pipe = Pipe(range(N)).map(ten_millis_identity, n_threads=n_threads)
        self.assertAlmostEqual(
            pipe.time(),
            TEN_MS * N / n_threads,
            delta=0.3 * (TEN_MS * N) / n_threads,
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

    def test_flatten(self):
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
            Pipe(range(8)).batch(max_size=3).collect(), [[0, 1, 2], [3, 4, 5], [6, 7]]
        )
        self.assertListEqual(
            Pipe(range(6)).batch(max_size=3).collect(), [[0, 1, 2], [3, 4, 5]]
        )
        self.assertListEqual(
            Pipe(range(8)).batch(max_size=1).collect(),
            list(map(lambda x: [x], range(8))),
        )
        self.assertListEqual(
            Pipe(range(8)).batch(max_size=8).collect(), [list(range(8))]
        )

    def test_slow(self):
        freq = 64
        pipe = Pipe(range(8)).map(ten_millis_identity).slow(freq)
        self.assertAlmostEqual(
            pipe.time(),
            1 / freq * 8,
            delta=0.3 * (1 / freq * 8),
        )

    def test_head(self):
        self.assertListEqual(Pipe(range(8)).head(3).collect(), [0, 1, 2])
        # stops after the second element
        self.assertAlmostEqual(
            Pipe(range(8)).map(ten_millis_identity).head(2).time(),
            TEN_MS * 2,
            delta=0.3 * TEN_MS * 2,
        )

    def test_collect(self):
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

    def test_time(self):
        new_pipe = lambda: Pipe(range(8)).slow(64)
        start_time = time.time()
        new_pipe().collect()
        execution_time = time.time() - start_time
        self.assertAlmostEqual(
            execution_time, new_pipe().time(), delta=0.3 * execution_time
        )

    def test_catch(self):
        # ignore = True
        self.assertListEqual(
            Pipe(["1", "r", "2"])
            .map(int)
            .catch(Exception, ignore=True)
            .map(type)
            .collect(),
            [int, int],
        )
        # ignore = False
        self.assertListEqual(
            Pipe(["1", "r", "2"]).map(int).catch(Exception).map(type).collect(),
            [int, ValueError, int],
        )
        self.assertListEqual(
            Pipe(["1", "r", "2"]).map(int).catch(ValueError).map(type).collect(),
            [int, ValueError, int],
        )
        # chain catches
        self.assertListEqual(
            Pipe(["1", "r", "2"])
            .map(int)
            .catch(TypeError)
            .catch(ValueError)
            .catch(TypeError)
            .map(type)
            .collect(),
            [int, ValueError, int],
        )
        self.assertDictEqual(
            dict(
                Counter(
                    Pipe(["1", "r", "2"])
                    .map(int, n_threads=2)
                    .catch(ValueError)
                    .map(type)
                    .collect()
                )
            ),
            dict(Counter([int, ValueError, int])),
        )
        self.assertDictEqual(
            dict(
                Counter(
                    Pipe(["1"])
                    .map(int)
                    .mix(Pipe(["r", "2"]).map(int))
                    .catch(ValueError)
                    .map(type)
                    .collect()
                )
            ),
            dict(Counter([int, ValueError, int])),
        )

        # raises
        self.assertRaises(
            ValueError,
            lambda: Pipe(["1", "r", "2"]).map(int).catch(TypeError).map(type).collect(),
        )
        self.assertRaises(
            ValueError,
            lambda: Pipe(["1"])
            .mix(Pipe(["r", "2"]).map(int))
            .catch(TypeError)
            .map(type)
            .collect(),
        )
        self.assertRaises(
            ValueError,
            lambda: Pipe(["1", "r", "2"])
            .map(int, n_threads=2)
            .catch(TypeError)
            .map(type)
            .collect(),
        )

    def test_superintend(self):
        Pipe("123").map(int).superintend()
        self.assertRaises(
            RuntimeError,
            lambda: Pipe("12-3").map(int).superintend(),
        )
