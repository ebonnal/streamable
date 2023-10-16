import itertools
import multiprocessing
import time
import timeit
import unittest
from collections import Counter

from typing import List, Optional, TypeVar

from parameterized import parameterized

from kioss import Pipe, util

TEN_MS = 0.01
DELTA = 1000
T = TypeVar("T")

def timepipe(pipe: Pipe):
    def iterate():
        for _ in pipe:
            pass
    return timeit.timeit(iterate, number=1)

# simulates an I/0 bound function
def ten_ms_identity(x: T) -> T:
    time.sleep(TEN_MS)
    return x

# size of the test collections
N = 64


class TestPipe(unittest.TestCase):
    def test_init(self):
        # from iterable
        self.assertListEqual(Pipe(range(8)).collect(), list(range(8)))
        # from iterator
        self.assertListEqual(Pipe(iter(range(8))).collect(), list(range(8)))

    def test_chain(self):
        # test that the order is preserved
        self.assertListEqual(
            Pipe(range(2))
            .chain(Pipe(range(2, 4)), Pipe(range(4, 6)))
            .chain(Pipe(range(6, 8)))
            .collect(),
            list(range(8)),
        )

    @parameterized.expand([[1], [2], [3]])
    def test_flatten(self, n_threads: Optional[int]):
        if n_threads is None:
            # test ordering
            self.assertListEqual(
                list(
                    Pipe(["Hello World", "Happy to be here :)"])
                    .map(str.split)
                    .map(iter)
                    .flatten(n_threads=n_threads)
                ),
                ["Hello", "World", "Happy", "to", "be", "here", ":)"],
            )
        self.assertSetEqual(
            set(
                Pipe(["Hello World", "Happy to be here :)"])
                .map(str.split)
                .map(iter)
                .flatten(n_threads=n_threads)
            ),
            {"Hello", "World", "Happy", "to", "be", "here", ":)"},
        )
        self.assertEqual(
            sum(
                Pipe([["1 2 3", "4 5 6"], ["7", "8 9 10"]])
                .map(iter)
                .flatten(n_threads=n_threads)
                .map(str.split)
                .map(iter)
                .flatten(n_threads=n_threads)
                .map(int)
            ),
            55,
        )

        # test potential recursion issue with chained empty iters
        Pipe([iter([]) for _ in range(2000)]).flatten(
            n_threads=n_threads
        ).collect()

        # test concurrency
        single_pipe_iteration_duration = 0.5
        queue_get_timeout = 0.1
        pipes = [
            Pipe(range(0, N, 3)).slow((N / 3) / single_pipe_iteration_duration),
            Pipe(range(1, N, 3)).slow((N / 3) / single_pipe_iteration_duration),
            Pipe(range(2, N, 3)).slow((N / 3) / single_pipe_iteration_duration),
        ]
        self.assertAlmostEqual(
            timeit.timeit(
                lambda: self.assertSetEqual(
                    set(
                        Pipe(pipes).flatten(
                            n_threads=n_threads
                        )
                    ),
                    set(range(N)),
                ),
                number=1,
            ),
            len(pipes)
            * single_pipe_iteration_duration
            / (1 if n_threads is None else n_threads),
            delta=DELTA
            * len(pipes)
            * single_pipe_iteration_duration
            / (1 if n_threads is None else n_threads)
            + queue_get_timeout,
        )

        # partial iteration

        zeros = lambda: Pipe([0] * N)
        self.assertEqual(next(Pipe([zeros(), zeros(), zeros()]).flatten(n_threads=n_threads)), 0)

        # exceptions in the middle on flattening is well catched, potential recursion issue too
        class RaisesStopIterationWhenCalledForIter:
            def __iter__(self):
                raise StopIteration()

        def raise_for_4(x):
            if x == 4:
                raise AssertionError()
            return x

        get_pipe = lambda: (
            Pipe(
                map(
                    raise_for_4,
                    [
                        map(int, "012-3-"),
                        3,
                        4,
                        RaisesStopIterationWhenCalledForIter(),
                        map(int, "-456"),
                    ],
                )
            ).map(iter).flatten(n_threads=n_threads)
        )
        self.assertSetEqual(
            set(get_pipe().catch(Exception, ignore=False).map(type)),
            {int, ValueError, TypeError, AssertionError, RuntimeError},
        )
        self.assertSetEqual(
            set(get_pipe().catch(Exception, ignore=True)),
            set(range(7)),
        )

        # test rasing:
        self.assertRaises(
            ValueError,
            Pipe([map(int, "12-3")])
            .flatten(n_threads=n_threads)
            .collect,
        )
        self.assertRaises(
            ValueError,
            Pipe(map(int, "-"))
            .flatten(n_threads=n_threads)
            .collect,
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

    @parameterized.expand([[1], [2], [3]])
    def test_map(self, n_threads: Optional[int]):
        func = lambda x: x**2
        self.assertSetEqual(
            set(
                Pipe(range(N))
                .map(ten_ms_identity, n_threads=n_threads)
                .map(lambda x: 1 / x)
                .map(func, n_threads=n_threads)
                .catch(
                    ZeroDivisionError, ignore=True
                )  # check that the ZeroDivisionError is bypass the call to func
                .map(ten_ms_identity, n_threads=n_threads)
            ),
            set(map(func, range(1, N))),
        )
        self.assertSetEqual(
            set(
                Pipe([[1], [], [3]])
                .map(iter)
                .map(next, n_threads=n_threads)
                .catch(RuntimeError, ignore=True)
            ),
            {1, 3},
        )

        # non-threaded vs threaded execution time
        pipe = Pipe(range(N)).map(ten_ms_identity)
        self.assertAlmostEqual(timepipe(pipe), TEN_MS * N, delta=DELTA * (TEN_MS * N))
        n_threads = 2
        pipe = Pipe(range(N)).map(ten_ms_identity, n_threads=n_threads)
        self.assertAlmostEqual(
            timepipe(pipe),
            TEN_MS * N / n_threads,
            delta=DELTA * (TEN_MS * N) / n_threads,
        )

    def test_do(self):
        l: List[int] = []

        func = lambda x: x**2

        def func_with_side_effect(x):
            res = func(x)
            l.append(res)
            return res

        args = range(N)
        self.assertListEqual(Pipe(args).do(func_with_side_effect).collect(), list(args))
        self.assertListEqual(l, list(map(func, args)))

        # with threads
        l.clear()
        self.assertSetEqual(
            set(Pipe(args).do(func_with_side_effect, n_threads=2)), set(args)
        )
        self.assertSetEqual(set(l), set(map(func, args)))

    def test_filter(self):
        self.assertListEqual(list(Pipe(range(8)).filter(lambda x: x % 2)), [1, 3, 5, 7])

        self.assertListEqual(list(Pipe(range(8)).filter(lambda _: False)), [])

    def test_batch(self):
        self.assertListEqual(
            Pipe(range(8)).batch(size=3).collect(), [[0, 1, 2], [3, 4, 5], [6, 7]]
        )
        self.assertListEqual(
            Pipe(range(6)).batch(size=3).collect(), [[0, 1, 2], [3, 4, 5]]
        )
        self.assertListEqual(
            Pipe(range(8)).batch(size=1).collect(),
            list(map(lambda x: [x], range(8))),
        )
        self.assertListEqual(Pipe(range(8)).batch(size=8).collect(), [list(range(8))])
        self.assertEqual(len(Pipe(range(8)).slow(10).batch(period=0.09).collect()), 7)
        # assert batch gracefully yields if next elem throw exception
        self.assertListEqual(
            Pipe("01234-56789")
            .map(int)
            .batch(2)
            .catch(ValueError, ignore=True)
            .collect(),
            [[0, 1], [2, 3], [4], [5, 6], [7, 8], [9]],
        )
        self.assertListEqual(
            Pipe("0123-56789")
            .map(int)
            .batch(2)
            .catch(ValueError, ignore=True)
            .collect(),
            [[0, 1], [2, 3], [5, 6], [7, 8], [9]],
        )
        self.assertListEqual(
            Pipe("0123-56789")
            .map(int)
            .batch(2)
            .catch(ValueError, ignore=False)
            .map(
                lambda potential_error: [potential_error]
                if isinstance(potential_error, Exception)
                else potential_error
            )
            .map(iter)
            .flatten()
            .map(type)
            .collect(),
            [int, int, int, int, ValueError, int, int, int, int, int],
        )

    @parameterized.expand([[1], [2], [3]])
    def test_slow(self, n_threads: Optional[int]):
        freq = 64
        pipe = (
            Pipe(range(N))
            .map(ten_ms_identity, n_threads=n_threads)
            .slow(freq)
        )
        self.assertAlmostEqual(
            timepipe(pipe),
            1 / freq * N,
            delta=DELTA * (1 / freq * N),
        )

    def test_collect(self):
        self.assertListEqual(Pipe(range(8)).collect(n_samples=6), list(range(6)))
        self.assertListEqual(Pipe(range(8)).collect(), list(range(8)))
        self.assertAlmostEqual(
            timeit.timeit(
                lambda: Pipe(range(8)).map(ten_ms_identity).collect(0),
                number=1,
            ),
            TEN_MS * 8,
            delta=DELTA * TEN_MS * 8,
        )

    def test_time(self):
        new_pipe = lambda: Pipe(range(8)).slow(64)
        start_time = time.time()
        new_pipe().collect()
        execution_time = time.time() - start_time
        self.assertAlmostEqual(
            execution_time, timepipe(new_pipe()), delta=DELTA * execution_time
        )

    @parameterized.expand([[1], [2], [3]])
    def test_catch(self, n_threads: Optional[int]):
        # ignore = True
        self.assertSetEqual(
            set(
                Pipe(["1", "r", "2"])
                .map(int, n_threads=n_threads)
                .catch(Exception, ignore=False)
                .map(type)
            ),
            {int, ValueError, int},
        )
        # ignore = False
        self.assertSetEqual(
            set(
                Pipe(["1", "r", "2"])
                .map(int, n_threads=n_threads)
                .catch(Exception)
                .map(type)
            ),
            {int, ValueError, int},
        )
        self.assertSetEqual(
            set(
                Pipe(["1", "r", "2"])
                .map(int, n_threads=n_threads)
                .catch(ValueError)
                .map(type)
            ),
            {int, ValueError, int},
        )
        # chain catches
        self.assertSetEqual(
            set(
                Pipe(["1", "r", "2"])
                .map(int, n_threads=n_threads)
                .catch(TypeError)
                .catch(ValueError)
                .catch(TypeError)
                .map(type)
            ),
            {int, ValueError, int},
        )
        self.assertDictEqual(
            dict(
                Counter(
                    Pipe(["1", "r", "2"])
                    .map(int, n_threads=n_threads)
                    .catch(ValueError)
                    .map(type)  # , n_threads=n_threads)
                    .collect()
                )
            ),
            dict(Counter([int, ValueError, int])),
        )

        # raises
        self.assertRaises(
            ValueError,
            Pipe(["1", "r", "2"])
            .map(int, n_threads=n_threads)
            .catch(TypeError)
            .map(type)
            .collect,
        )
        self.assertRaises(
            ValueError,
            Pipe(["1", "r", "2"])
            .map(int, n_threads=n_threads)
            .catch(TypeError)
            .map(type)
            .collect,
        )

    def test_superintend(self):
        self.assertRaises(
            ValueError,
            Pipe("12-3").map(int).superintend,
        )
        self.assertListEqual(Pipe("123").map(int).superintend(n_samples=2), [1, 2])

    def test_log(self):
        self.assertListEqual(
            Pipe("123")
            .log("chars")
            .map(int)
            .log("ints")
            .batch(2)
            .log("ints_pairs")
            .collect(),
            [[1, 2], [3]],
        )

    def test_partial_iteration(self):
        first_elem = next(
            Pipe([0] * N)
            .slow(50)
            .map(util.identity, n_threads=2)
            .slow(50)
            .map(util.identity, n_threads=2)
            .slow(50)
            .map(util.identity, n_threads=2)
            .slow(50)
        )
        self.assertEqual(first_elem, 0)
        n = 10
        pipe = (
            Pipe([0] * N)
            .slow(50)
            .map(util.identity, n_threads=2)
            .slow(50)
            .map(util.identity, n_threads=2)
            .slow(50)
            .map(util.identity, n_threads=2)
            .slow(50)
        )
        samples = list(itertools.islice(pipe, n))
        self.assertListEqual(samples, [0] * n)
