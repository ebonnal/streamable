import itertools
import time
import timeit
import unittest
from collections import Counter

from typing import List, Optional, TypeVar

from parameterized import parameterized

from kioss import Pipe, _util

TEN_MS = 0.01
DELTA = 0.3
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
        self.assertListEqual(Pipe(range(8).__iter__).collect(), list(range(8)))
        # from iterator
        self.assertListEqual(Pipe(range(8).__iter__).collect(), list(range(8)))

    def test_chain(self):
        # test that the order is preserved
        self.assertListEqual(
            Pipe(range(2).__iter__)
            .chain(Pipe(range(2, 4).__iter__), Pipe(range(4, 6).__iter__))
            .chain(Pipe(range(6, 8).__iter__))
            .collect(),
            list(range(8)),
        )

    @parameterized.expand([[1], [2], [3]])
    def test_flatten(self, n_threads: int):
        if n_threads == 1:
            # test ordering
            self.assertListEqual(
                list(
                    Pipe(["Hello World", "Happy to be here :)"].__iter__)
                    .map(str.split)
                    .map(iter)
                    .flatten(n_threads=n_threads)
                ),
                ["Hello", "World", "Happy", "to", "be", "here", ":)"],
            )
        self.assertSetEqual(
            set(
                Pipe(["Hello World", "Happy to be here :)"].__iter__)
                .map(str.split)
                .map(iter)
                .flatten(n_threads=n_threads)
            ),
            {"Hello", "World", "Happy", "to", "be", "here", ":)"},
        )
        self.assertEqual(
            sum(
                Pipe([["1 2 3", "4 5 6"], ["7", "8 9 10"]].__iter__)
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
        Pipe([iter([]) for _ in range(2000)].__iter__).flatten(
            n_threads=n_threads
        ).collect()

        # test concurrency
        single_pipe_iteration_duration = 0.5
        queue_get_timeout = 0.1
        pipes = [
            Pipe(range(0, N, 3).__iter__).slow((N / 3) / single_pipe_iteration_duration),
            Pipe(range(1, N, 3).__iter__).slow((N / 3) / single_pipe_iteration_duration),
            Pipe(range(2, N, 3).__iter__).slow((N / 3) / single_pipe_iteration_duration),
        ]
        self.assertAlmostEqual(
            timeit.timeit(
                lambda: self.assertSetEqual(
                    set(Pipe(pipes.__iter__).map(iter).flatten(n_threads=n_threads)),
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

        zeros = lambda: Pipe(([0] * N).__iter__)
        self.assertEqual(
            next(
                iter(
                    Pipe([zeros(), zeros(), zeros()].__iter__)
                    .map(iter)
                    .flatten(n_threads=n_threads)
                )
            ),
            0,
        )

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
                lambda: map(
                    raise_for_4,
                    [
                        map(int, "012-3-"),
                        3,
                        4,
                        RaisesStopIterationWhenCalledForIter(),
                        map(int, "-456"),
                    ],
                )
            )
            .map(iter)
            .flatten(n_threads=n_threads)
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
            Pipe([map(int, "12-3")].__iter__).flatten(n_threads=n_threads).collect,
        )
        self.assertRaises(
            ValueError,
            Pipe(lambda: map(int, "-")).flatten(n_threads=n_threads).collect,
        )

    def test_add(self):
        self.assertListEqual(
            list(
                sum(
                    [
                        Pipe(range(0, 2).__iter__),
                        Pipe(range(2, 4).__iter__),
                        Pipe(range(4, 6).__iter__),
                        Pipe(range(6, 8).__iter__),
                    ],
                    start=Pipe([].__iter__),
                )
            ),
            list(range(8)),
        )

    @parameterized.expand([[1], [2], [3]])
    def test_map(self, n_threads: int):
        func = lambda x: x**2
        self.assertSetEqual(
            set(
                Pipe(range(N).__iter__)
                .map(ten_ms_identity, n_threads=n_threads)
                .map(lambda x: x if 1 / x else x)
                .map(func, n_threads=n_threads)
                .catch(ZeroDivisionError, ignore=True)
                .map(
                    ten_ms_identity, n_threads=n_threads
                )  # check that the ZeroDivisionError is bypass the call to func
            ),
            set(map(func, range(1, N))),
        )
        self.assertSetEqual(
            set(
                Pipe([[1], [], [3]].__iter__)
                .map(iter)
                .map(next, n_threads=n_threads)
                .catch(RuntimeError, ignore=True)
            ),
            {1, 3},
        )

    def test_map_threading_bench(self):
        # non-threaded vs threaded execution time
        pipe = Pipe(range(N).__iter__).map(ten_ms_identity)
        self.assertAlmostEqual(timepipe(pipe), TEN_MS * N, delta=DELTA * (TEN_MS * N))
        n_threads = 2
        pipe = Pipe(range(N).__iter__).map(ten_ms_identity, n_threads=n_threads)
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
        self.assertListEqual(
            Pipe(args.__iter__).do(func_with_side_effect).collect(), list(args)
        )
        self.assertListEqual(l, list(map(func, args)))

        # with threads
        l.clear()
        self.assertSetEqual(
            set(Pipe(args.__iter__).do(func_with_side_effect, n_threads=2)), set(args)
        )
        self.assertSetEqual(set(l), set(map(func, args)))

    def test_filter(self):
        self.assertListEqual(
            list(Pipe(range(8).__iter__).filter(lambda x: x % 2)), [1, 3, 5, 7]
        )

        self.assertListEqual(list(Pipe(range(8).__iter__).filter(lambda _: False)), [])

    def test_batch(self):
        self.assertListEqual(
            Pipe(range(8).__iter__).batch(size=3).collect(),
            [[0, 1, 2], [3, 4, 5], [6, 7]],
        )
        self.assertListEqual(
            Pipe(range(6).__iter__).batch(size=3).collect(), [[0, 1, 2], [3, 4, 5]]
        )
        self.assertListEqual(
            Pipe(range(8).__iter__).batch(size=1).collect(),
            list(map(lambda x: [x], range(8))),
        )
        self.assertListEqual(
            Pipe(range(8).__iter__).batch(size=8).collect(), [list(range(8))]
        )
        self.assertEqual(
            len(Pipe(range(8).__iter__).slow(10).batch(period=0.09).collect()), 7
        )
        # assert batch gracefully yields if next elem throw exception
        self.assertListEqual(
            Pipe("01234-56789".__iter__)
            .map(int)
            .batch(2)
            .catch(ValueError, ignore=True)
            .collect(),
            [[0, 1], [2, 3], [4], [5, 6], [7, 8], [9]],
        )
        self.assertListEqual(
            Pipe("0123-56789".__iter__)
            .map(int)
            .batch(2)
            .catch(ValueError, ignore=True)
            .collect(),
            [[0, 1], [2, 3], [5, 6], [7, 8], [9]],
        )
        self.assertListEqual(
            Pipe("0123-56789".__iter__)
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
    def test_slow(self, n_threads: int):
        freq = 64
        pipe = (
            Pipe(range(N).__iter__).map(ten_ms_identity, n_threads=n_threads).slow(freq)
        )
        self.assertAlmostEqual(
            timepipe(pipe),
            1 / freq * N,
            delta=DELTA * (1 / freq * N),
        )

    def test_collect(self):
        self.assertListEqual(
            Pipe(range(8).__iter__).collect(n_samples=6), list(range(6))
        )
        self.assertListEqual(Pipe(range(8).__iter__).collect(), list(range(8)))
        self.assertAlmostEqual(
            timeit.timeit(
                lambda: Pipe(range(8).__iter__).map(ten_ms_identity).collect(0),
                number=1,
            ),
            TEN_MS * 8,
            delta=DELTA * TEN_MS * 8,
        )

    def test_time(self):
        new_pipe = lambda: Pipe(range(8).__iter__).slow(64)
        start_time = time.time()
        new_pipe().collect()
        execution_time = time.time() - start_time
        self.assertAlmostEqual(
            execution_time, timepipe(new_pipe()), delta=DELTA * execution_time
        )

    @parameterized.expand([[1], [2], [3]])
    def test_catch(self, n_threads: int):
        # ignore = True
        self.assertSetEqual(
            set(
                Pipe(["1", "r", "2"].__iter__)
                .map(int, n_threads=n_threads)
                .catch(Exception, ignore=False)
                .map(type)
            ),
            {int, ValueError, int},
        )
        # ignore = False
        self.assertSetEqual(
            set(
                Pipe(["1", "r", "2"].__iter__)
                .map(int, n_threads=n_threads)
                .catch(Exception)
                .map(type)
            ),
            {int, ValueError, int},
        )
        self.assertSetEqual(
            set(
                Pipe(["1", "r", "2"].__iter__)
                .map(int, n_threads=n_threads)
                .catch(ValueError)
                .map(type)
            ),
            {int, ValueError, int},
        )
        # chain catches
        self.assertSetEqual(
            set(
                Pipe(["1", "r", "2"].__iter__)
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
                    Pipe(["1", "r", "2"].__iter__)
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
            Pipe(["1", "r", "2"].__iter__)
            .map(int, n_threads=n_threads)
            .catch(TypeError)
            .map(type)
            .collect,
        )
        self.assertRaises(
            ValueError,
            Pipe(["1", "r", "2"].__iter__)
            .map(int, n_threads=n_threads)
            .catch(TypeError)
            .map(type)
            .collect,
        )

    def test_superintend(self):
        self.assertRaises(
            ValueError,
            Pipe("12-3".__iter__).map(int).superintend,
        )
        self.assertListEqual(
            Pipe("123".__iter__).map(int).superintend(n_samples=2), [1, 2]
        )

    def test_log(self):
        self.assertListEqual(
            Pipe("123".__iter__)
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
            iter(
                Pipe(([0] * N).__iter__)
                .slow(50)
                .map(_util.identity, n_threads=2)
                .slow(50)
                .map(_util.identity, n_threads=2)
                .slow(50)
                .map(_util.identity, n_threads=2)
                .slow(50)
            )
        )
        self.assertEqual(first_elem, 0)
        n = 10
        pipe = (
            Pipe(([0] * N).__iter__)
            .slow(50)
            .map(_util.identity, n_threads=2)
            .slow(50)
            .map(_util.identity, n_threads=2)
            .slow(50)
            .map(_util.identity, n_threads=2)
            .slow(50)
        )
        samples = list(itertools.islice(pipe, n))
        self.assertListEqual(samples, [0] * n)

    def test_invalid_source(self):
        self.assertRaises(TypeError, lambda: Pipe(range(3)))
        pipe_ok_at_construction = Pipe(lambda: range(3))
        self.assertRaises(TypeError, lambda: pipe_ok_at_construction.collect())

    @parameterized.expand([[1], [2], [3]])
    def test_invalid_flatten_upstream(self, n_threads: int):
        self.assertEqual(
            Pipe(range(3).__iter__)
            .flatten(n_threads=n_threads)
            .catch(TypeError)  # important to check potential infinite recursion
            .map(type)
            .collect(),
            [TypeError] * 3,
        )

    def test_planning_and_execution_decoupling(self):
        a = Pipe(range(N).__iter__)
        b = a.batch(size=N)
        # test double execution
        self.assertListEqual(a.collect(), list(range(N)))
        self.assertListEqual(a.collect(), list(range(N)))
        # test b not affected by a execution
        self.assertListEqual(b.collect(), [list(range(N))])

    def test_generator_already_generating(self):
        self.assertEqual(
            Counter(
                Pipe(
                    [(ten_ms_identity(x) for x in range(N)) for _ in range(3)].__iter__
                ).flatten(n_threads=2)
            ),
            Counter(list(range(N)) + list(range(N)) + list(range(N))),
        )
