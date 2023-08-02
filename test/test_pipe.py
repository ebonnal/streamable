import itertools
import multiprocessing
import time
import timeit
import unittest
from collections import Counter
from functools import reduce
from typing import List, Optional, TypeVar

from parameterized import parameterized

from kioss import Pipe, util

TEN_MS = 0.01

T = TypeVar("T")


def ten_millis_identity(x: T) -> T:
    time.sleep(TEN_MS)
    return x


non_local_l = multiprocessing.Manager().list()


def non_local_func(x):
    return x**2


def non_local_func_with_side_effect(x):
    res = non_local_func(x)
    non_local_l.append(res)
    return res


Pipe._MAX_NUM_WAITING_ELEMS_PER_WORKER = 2
N = 64


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

    @parameterized.expand(
        [[worker_type] for worker_type in Pipe.SUPPORTED_WORKER_TYPES]
    )
    def test_mix(self, worker_type: str):
        single_pipe_iteration_duration = 0.5
        queue_get_timeout = 0.1
        new_pipes = lambda: [
            Pipe(range(0, N, 3)).slow((N / 3) / single_pipe_iteration_duration),
            Pipe(range(1, N, 3)).slow((N / 3) / single_pipe_iteration_duration),
            Pipe(range(2, N, 3)).slow((N / 3) / single_pipe_iteration_duration),
        ]
        self.assertAlmostEqual(
            timeit.timeit(
                lambda: self.assertSetEqual(
                    set(Pipe[int]().mix(*new_pipes(), worker_type=worker_type)),
                    set(range(N)),
                ),
                number=1,
            ),
            single_pipe_iteration_duration,
            delta=0.3 * single_pipe_iteration_duration + queue_get_timeout,
        )
        # same perf if chaining mix methods or if mixing in a single one
        pipes = new_pipes()
        self.assertSetEqual(
            set(Pipe().mix(*new_pipes(), worker_type=worker_type)),
            set(reduce(Pipe.mix, pipes[1:], Pipe(pipes[0]))),
        )
        pipes = new_pipes()
        self.assertAlmostEqual(
            Pipe().mix(*new_pipes(), worker_type=worker_type).time(),
            reduce(Pipe.mix, pipes[1:], Pipe(pipes[0])).time(),
            delta=0.05 * Pipe().mix(*new_pipes(), worker_type=worker_type).time()
            + queue_get_timeout,
        )
        # partial iteration after mix
        with Pipe().mix(*new_pipes(), worker_type=worker_type) as pipe:
            self.assertEqual(next(pipe), 0)

        # only Pipes should be passed
        self.assertRaises(TypeError, lambda: Pipe().mix(range(10)))

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

    @parameterized.expand(
        [
            [
                non_local_func
                if worker_type == Pipe.PROCESS_WORKER_TYPE
                else lambda x: x**2,
                n_workers,
                worker_type,
            ]
            for n_workers in [None, 1]
            for worker_type in Pipe.SUPPORTED_WORKER_TYPES
        ]
    )
    def test_map(self, func, n_workers: Optional[int], worker_type: str):
        self.assertSetEqual(
            set(
                Pipe(range(N))
                .map(ten_millis_identity, n_workers=n_workers, worker_type=worker_type)
                .map(lambda x: x if 1 / x is not None else None)
                .map(func, n_workers=n_workers, worker_type=worker_type)
                .catch(
                    ZeroDivisionError, ignore=True
                )  # check that the ZeroDivisionError is bypass the call to func
                .map(ten_millis_identity, n_workers=n_workers, worker_type=worker_type)
            ),
            set(map(func, range(1, N))),
        )

    def test_map_timing(self):
        # non-threaded vs threaded execution time
        pipe = Pipe(range(N)).map(ten_millis_identity)
        self.assertAlmostEqual(pipe.time(), TEN_MS * N, delta=0.3 * (TEN_MS * N))
        n_workers = 2
        pipe = Pipe(range(N)).map(ten_millis_identity, n_workers=n_workers)
        self.assertAlmostEqual(
            pipe.time(),
            TEN_MS * N / n_workers,
            delta=0.3 * (TEN_MS * N) / n_workers,
        )

    def test__apply(self):
        self.assertRaisesRegex(
            AttributeError,
            r"Can't pickle local object 'TestPipe\.test__apply\.<locals>\.<lambda>\.<locals>\.<lambda>'",
            lambda: Pipe([]).map(
                lambda x: x, n_workers=1, worker_type=Pipe.PROCESS_WORKER_TYPE
            ),
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
            set(Pipe(args).do(func_with_side_effect, n_workers=2)), set(args)
        )
        self.assertSetEqual(set(l), set(map(func, args)))

        # with processes
        while len(non_local_l):
            non_local_l.pop()
        self.assertSetEqual(
            set(
                Pipe(args).do(
                    non_local_func_with_side_effect,
                    n_workers=2,
                    worker_type=Pipe.PROCESS_WORKER_TYPE,
                )
            ),
            set(args),
        )
        self.assertSetEqual(set(l), set(map(func, args)))

        # with_processes and with slow upstream
        while len(non_local_l):
            non_local_l.pop()
        self.assertSetEqual(
            set(
                Pipe(range(N))
                .map(ten_millis_identity)
                .do(
                    non_local_func_with_side_effect,
                    n_workers=8,
                    worker_type=Pipe.PROCESS_WORKER_TYPE,
                )
            ),
            set(range(N)),
        )
        self.assertSetEqual(set(l), set(map(func, args)))

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
        self.assertEqual(len(Pipe(range(8)).slow(10).batch(secs=0.09).collect()), 7)
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
            .flatten()
            .map(type)
            .collect(),
            [int, int, int, int, ValueError, int, int, int, int, int],
        )

    @parameterized.expand(
        [
            [n_workers, worker_type]
            for n_workers in [None, 1]
            for worker_type in Pipe.SUPPORTED_WORKER_TYPES
        ]
    )
    def test_slow(self, n_workers: Optional[int], worker_type: str):
        freq = 64
        pipe = (
            Pipe(range(N))
            .map(ten_millis_identity, n_workers=n_workers, worker_type=worker_type)
            .slow(freq)
        )
        self.assertAlmostEqual(
            pipe.time(),
            1 / freq * N,
            delta=0.3 * (1 / freq * N),
        )

    def test_collect(self):
        self.assertListEqual(Pipe(range(8)).collect(n_samples=6), list(range(6)))
        self.assertListEqual(Pipe(range(8)).collect(), list(range(8)))
        self.assertAlmostEqual(
            timeit.timeit(
                lambda: Pipe(range(8)).map(ten_millis_identity).collect(0),
                number=1,
            ),
            TEN_MS * 8,
            delta=0.3 * TEN_MS * 8,
        )

    def test_time(self):
        new_pipe = lambda: Pipe(range(8)).slow(64)
        start_time = time.time()
        new_pipe().collect()
        execution_time = time.time() - start_time
        self.assertAlmostEqual(
            execution_time, new_pipe().time(), delta=0.3 * execution_time
        )

    @parameterized.expand(
        [
            [n_workers, worker_type]
            for n_workers in [None, 1]
            for worker_type in Pipe.SUPPORTED_WORKER_TYPES
        ]
    )
    def test_catch(self, n_workers: Optional[int], worker_type: str):
        # ignore = True
        self.assertSetEqual(
            set(
                Pipe(["1", "r", "2"])
                .map(int, n_workers=n_workers, worker_type=worker_type)
                .catch(Exception, ignore=False)
                .map(type)
            ),
            {int, ValueError, int},
        )
        # ignore = False
        self.assertSetEqual(
            set(
                Pipe(["1", "r", "2"])
                .map(int, n_workers=n_workers, worker_type=worker_type)
                .catch(Exception)
                .map(type)
            ),
            {int, ValueError, int},
        )
        self.assertSetEqual(
            set(
                Pipe(["1", "r", "2"])
                .map(int, n_workers=n_workers, worker_type=worker_type)
                .catch(ValueError)
                .map(type)
            ),
            {int, ValueError, int},
        )
        # chain catches
        self.assertSetEqual(
            set(
                Pipe(["1", "r", "2"])
                .map(int, n_workers=n_workers, worker_type=worker_type)
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
                    Pipe(["1"])
                    .map(int, n_workers=n_workers, worker_type=worker_type)
                    .mix(Pipe(["r", "2"]).map(int))
                    .catch(ValueError)
                    .map(type)  # , n_workers=n_workers, worker_type=worker_type)
                    .collect()
                )
            ),
            dict(Counter([int, ValueError, int])),
        )

        # raises
        self.assertRaises(
            ValueError,
            lambda: Pipe(["1", "r", "2"])
            .map(int, n_workers=n_workers, worker_type=worker_type)
            .catch(TypeError)
            .map(type)
            .collect(),
        )
        self.assertRaises(
            ValueError,
            lambda: Pipe(["1"])
            .mix(
                Pipe(["r", "2"]).map(int, n_workers=n_workers, worker_type=worker_type)
            )
            .catch(TypeError)
            .map(type)
            .collect(),
        )

    def test_superintend(self):
        Pipe("123").map(int).log("custom named elements").superintend()
        self.assertRaises(
            ValueError,
            lambda: Pipe("12-3").map(int).superintend(),
        )

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

    @parameterized.expand(
        [[worker_type] for worker_type in Pipe.SUPPORTED_WORKER_TYPES]
    )
    def test_partial_iteration(self, worker_type: str):
        with Pipe([0] * N).slow(50).mix(
            Pipe([0] * N)
            .slow(50)
            .map(util.identity, worker_type=worker_type, n_workers=2)
            .slow(50),
            worker_type=worker_type,
        ).map(util.identity, worker_type=worker_type, n_workers=2).slow(50).map(
            util.identity, worker_type=worker_type, n_workers=2
        ).slow(
            50
        ) as pipe:
            first_elem = next(pipe)
        self.assertEqual(first_elem, 0)
        n = 10
        with Pipe([0] * N).slow(50).mix(
            Pipe([0] * N)
            .slow(50)
            .map(util.identity, worker_type=worker_type, n_workers=2)
            .slow(50),
            worker_type=worker_type,
        ).map(util.identity, worker_type=worker_type, n_workers=2).slow(50).map(
            util.identity, worker_type=worker_type, n_workers=2
        ).slow(
            50
        ) as pipe:
            samples = list(itertools.islice(pipe, n))
        self.assertListEqual(samples, [0] * n)
