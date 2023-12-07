import itertools
import time
import timeit
import unittest
from collections import Counter
from typing import Callable, Iterator, List, TypeVar

from parameterized import parameterized  # type: ignore

from kioss import Pipe, _util

TEN_MS = 0.01
DELTA = 0.35
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
    def test_init(self) -> None:
        # from iterable
        self.assertListEqual(
            Pipe.from_source(range(8).__iter__).collect(), list(range(8))
        )
        # from iterator
        self.assertListEqual(
            Pipe.from_source(range(8).__iter__).collect(), list(range(8))
        )

    def test_chain(self) -> None:
        # test that the order is preserved
        self.assertListEqual(
            Pipe.from_source(range(2).__iter__)
            .chain(
                Pipe.from_source(range(2, 4).__iter__),
                Pipe.from_source(range(4, 6).__iter__),
            )
            .chain(Pipe.from_source(range(6, 8).__iter__))
            .collect(),
            list(range(8)),
        )

    def test_flatten_typing(self) -> None:
        a: Pipe[str] = Pipe.from_source("abc".__iter__).map(iter).flatten()
        b: Pipe[str] = Pipe.from_source("abc".__iter__).map(list).flatten()
        c: Pipe[str] = Pipe.from_source("abc".__iter__).map(set).flatten()

    @parameterized.expand([[1], [2], [3]])
    def test_flatten(self, n_threads: int):
        if n_threads == 1:
            # test ordering
            self.assertListEqual(
                list(
                    Pipe.from_source(["Hello World", "Happy to be here :)"].__iter__)
                    .map(str.split)
                    .flatten(n_threads=n_threads)
                ),
                ["Hello", "World", "Happy", "to", "be", "here", ":)"],
            )
        self.assertSetEqual(
            set(
                Pipe.from_source(["Hello World", "Happy to be here :)"].__iter__)
                .map(str.split)
                .flatten(n_threads=n_threads)
            ),
            {"Hello", "World", "Happy", "to", "be", "here", ":)"},
        )
        self.assertEqual(
            sum(
                Pipe.from_source([["1 2 3", "4 5 6"], ["7", "8 9 10"]].__iter__)
                .flatten(n_threads=n_threads)
                .map(str.split)
                .flatten(n_threads=n_threads)
                .map(int)
            ),
            55,
        )

        # test potential recursion issue with chained empty iters
        Pipe.from_source([iter([]) for _ in range(2000)].__iter__).flatten(
            n_threads=n_threads
        ).collect()

        # test concurrency
        single_pipe_iteration_duration = 0.5
        queue_get_timeout = 0.1
        pipes = [
            Pipe.from_source(range(0, N, 3).__iter__).slow(
                (N / 3) / single_pipe_iteration_duration
            ),
            Pipe.from_source(range(1, N, 3).__iter__).slow(
                (N / 3) / single_pipe_iteration_duration
            ),
            Pipe.from_source(range(2, N, 3).__iter__).slow(
                (N / 3) / single_pipe_iteration_duration
            ),
        ]
        self.assertAlmostEqual(
            timeit.timeit(
                lambda: self.assertSetEqual(
                    set(Pipe.from_source(pipes.__iter__).flatten(n_threads=n_threads)),
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

        zeros = lambda: Pipe.from_source(([0] * N).__iter__)
        self.assertEqual(
            next(
                iter(
                    Pipe.from_source([zeros(), zeros(), zeros()].__iter__).flatten(
                        n_threads=n_threads
                    )
                )
            ),
            0,
        )

        # exceptions in the middle on flattening is well catched, potential recursion issue too
        class RaisesStopIterationWhenCalledForIter:
            def __iter__(self) -> None:
                raise StopIteration()

        def raise_for_4(x):
            if x == 4:
                raise AssertionError()
            return x

        get_pipe: Callable[[], Pipe[int]] = lambda: (
            Pipe.from_source(
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
        error_types = set()

        def store_error_types(error):
            error_types.add(type(error))
            return True

        set(get_pipe().catch(Exception, when=store_error_types))
        self.assertSetEqual(
            error_types,
            {ValueError, TypeError, AssertionError, RuntimeError},
        )
        self.assertSetEqual(
            set(get_pipe().catch(Exception)),
            set(range(7)),
        )

        # test rasing:
        self.assertRaises(
            ValueError,
            Pipe.from_source([map(int, "12-3")].__iter__).flatten(n_threads=n_threads).collect,  # type: ignore
        )
        self.assertRaises(
            ValueError,
            Pipe.from_source(lambda: map(int, "-")).flatten(n_threads=n_threads).collect,  # type: ignore
        )

    def test_add(self) -> None:
        self.assertListEqual(
            list(
                sum(
                    [
                        Pipe.from_source(range(0, 2).__iter__),
                        Pipe.from_source(range(2, 4).__iter__),
                        Pipe.from_source(range(4, 6).__iter__),
                        Pipe.from_source(range(6, 8).__iter__),
                    ],
                    start=Pipe.from_source([].__iter__),
                )
            ),
            list(range(8)),
        )

    @parameterized.expand([[1], [2], [3]])
    def test_map(self, n_threads: int):
        func = lambda x: x**2
        self.assertSetEqual(
            set(
                Pipe.from_source(range(N).__iter__)
                .map(ten_ms_identity, n_threads=n_threads)
                .map(lambda x: x if 1 / x else x)
                .map(func, n_threads=n_threads)
                .catch(ZeroDivisionError)
                .map(
                    ten_ms_identity, n_threads=n_threads
                )  # check that the ZeroDivisionError is bypass the call to func
            ),
            set(map(func, range(1, N))),
        )
        l: List[List[int]] = [[1], [], [3]]
        self.assertSetEqual(
            set(
                Pipe.from_source(l.__iter__)
                .map(lambda l: iter(l))
                .map(next, n_threads=n_threads)
                .catch(RuntimeError)
            ),
            {1, 3},
        )

    def test_map_threading_bench(self) -> None:
        # non-threaded vs threaded execution time
        pipe = Pipe.from_source(range(N).__iter__).map(ten_ms_identity)
        self.assertAlmostEqual(timepipe(pipe), TEN_MS * N, delta=DELTA * (TEN_MS * N))
        n_threads = 2
        pipe = Pipe.from_source(range(N).__iter__).map(
            ten_ms_identity, n_threads=n_threads
        )
        self.assertAlmostEqual(
            timepipe(pipe),
            TEN_MS * N / n_threads,
            delta=DELTA * (TEN_MS * N) / n_threads,
        )

    def test_do(self) -> None:
        l: List[int] = []

        func = lambda x: x**2

        def func_with_side_effect(x):
            res = func(x)
            l.append(res)
            return res

        args = range(N)
        self.assertListEqual(
            Pipe.from_source(args.__iter__).do(func_with_side_effect).collect(),
            list(args),
        )
        self.assertListEqual(l, list(map(func, args)))

        # with threads
        l.clear()
        self.assertSetEqual(
            set(Pipe.from_source(args.__iter__).do(func_with_side_effect, n_threads=2)),
            set(args),
        )
        self.assertSetEqual(set(l), set(map(func, args)))

    def test_filter(self) -> None:
        self.assertListEqual(
            list(Pipe.from_source(range(8).__iter__).filter(lambda x: x % 2 != 0)),
            [1, 3, 5, 7],
        )

        self.assertListEqual(
            list(Pipe.from_source(range(8).__iter__).filter(lambda _: False)), []
        )

    def test_batch(self) -> None:
        self.assertListEqual(
            Pipe.from_source(range(8).__iter__).batch(size=3).collect(),
            [[0, 1, 2], [3, 4, 5], [6, 7]],
        )
        self.assertListEqual(
            Pipe.from_source(range(6).__iter__).batch(size=3).collect(),
            [[0, 1, 2], [3, 4, 5]],
        )
        self.assertListEqual(
            Pipe.from_source(range(8).__iter__).batch(size=1).collect(),
            list(map(lambda x: [x], range(8))),
        )
        self.assertListEqual(
            Pipe.from_source(range(8).__iter__).batch(size=8).collect(),
            [list(range(8))],
        )
        self.assertEqual(
            len(
                Pipe.from_source(range(8).__iter__)
                .slow(10)
                .batch(period=0.09)
                .collect()
            ),
            7,
        )
        # assert batch gracefully yields if next elem throw exception
        self.assertListEqual(
            Pipe.from_source("01234-56789".__iter__)
            .map(int)
            .batch(2)
            .catch(ValueError)
            .collect(),
            [[0, 1], [2, 3], [4], [5, 6], [7, 8], [9]],
        )
        self.assertListEqual(
            Pipe.from_source("0123-56789".__iter__)
            .map(int)
            .batch(2)
            .catch(ValueError)
            .collect(),
            [[0, 1], [2, 3], [5, 6], [7, 8], [9]],
        )
        errors = set()

        def store_errors(error):
            errors.add(error)
            return True

        self.assertListEqual(
            Pipe.from_source("0123-56789".__iter__)
            .map(int)
            .batch(2)
            .catch(ValueError, when=store_errors)
            .flatten()
            .map(type)
            .collect(),
            [int, int, int, int, int, int, int, int, int],
        )
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(next(iter(errors)), ValueError)

    @parameterized.expand([[1], [2], [3]])
    def test_slow(self, n_threads: int):
        freq = 64
        pipe = (
            Pipe.from_source(range(N).__iter__)
            .map(ten_ms_identity, n_threads=n_threads)
            .slow(freq)
        )
        self.assertAlmostEqual(
            timepipe(pipe),
            1 / freq * N,
            delta=DELTA * (1 / freq * N),
        )

    def test_collect(self) -> None:
        self.assertListEqual(
            Pipe.from_source(range(8).__iter__).collect(n_samples=6), list(range(6))
        )
        self.assertListEqual(
            Pipe.from_source(range(8).__iter__).collect(), list(range(8))
        )
        self.assertAlmostEqual(
            timeit.timeit(
                lambda: Pipe.from_source(range(8).__iter__)
                .map(ten_ms_identity)
                .collect(0),
                number=1,
            ),
            TEN_MS * 8,
            delta=DELTA * TEN_MS * 8,
        )

    def test_time(self) -> None:
        new_pipe = lambda: Pipe.from_source(range(8).__iter__).slow(64)
        start_time = time.time()
        new_pipe().collect()
        execution_time = time.time() - start_time
        self.assertAlmostEqual(
            execution_time, timepipe(new_pipe()), delta=DELTA * execution_time
        )

    @parameterized.expand([[1], [2], [3]])
    def test_catch(self, n_threads: int):
        # ignore = True
        errors = set()

        def store_errors(error):
            errors.add(error)
            return True

        self.assertSetEqual(
            set(
                Pipe.from_source(["1", "r", "2"].__iter__)
                .map(int, n_threads=n_threads)
                .catch(
                    Exception,
                    when=lambda error: "invalid literal for int() with base 10:"
                    not in str(error),
                )
                .catch(Exception, when=store_errors)
                .map(type)
            ),
            {int},
        )
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(next(iter(errors)), ValueError)

        self.assertRaises(
            ValueError,
            (
                Pipe.from_source(["1", "r", "2"].__iter__)
                .map(int, n_threads=n_threads)
                .catch(ValueError, when=lambda error: False)
                .collect
            ),
        )
        self.assertListEqual(
            list(
                Pipe.from_source(["1", "r", "2"].__iter__)
                .map(int, n_threads=n_threads)
                .catch(
                    ValueError,
                    when=lambda error: "invalid literal for int() with base 10:"
                    in str(error),
                )
                .map(type)
            ),
            [int, int],
        )
        # chain catches
        self.assertListEqual(
            list(
                Pipe.from_source(["1", "r", "2"].__iter__)
                .map(int, n_threads=n_threads)
                .catch(TypeError)
                .catch(ValueError)
                .catch(TypeError)
                .map(type)
            ),
            [int, int],
        )

        # raises
        self.assertRaises(
            ValueError,
            Pipe.from_source(["1", "r", "2"].__iter__)
            .map(int, n_threads=n_threads)
            .catch(TypeError)
            .map(type)
            .collect,
        )
        self.assertRaises(
            ValueError,
            Pipe.from_source(["1", "r", "2"].__iter__)
            .map(int, n_threads=n_threads)
            .catch(TypeError)
            .map(type)
            .collect,
        )

    def test_superintend(self) -> None:
        self.assertListEqual(
            Pipe.from_source("123".__iter__).map(int).superintend(n_samples=2), [1, 2]
        )

        # errors
        superintend = Pipe.from_source("12-3".__iter__).map(int).superintend
        self.assertRaises(
            ValueError,
            superintend,
        )
        # does not raise with sufficient threshold
        superintend(raise_if_more_errors_than=1)
        # raise with insufficient threshold
        self.assertRaises(
            ValueError,
            lambda: superintend(raise_if_more_errors_than=0),
        )

        # fail_fast
        self.assertRaises(
            ValueError,
            lambda: Pipe.from_source("a-b".__iter__)
            .map(int)
            .superintend(fail_fast=True),
        )

    def test_log(self) -> None:
        self.assertListEqual(
            Pipe.from_source("123".__iter__)
            .log("chars")
            .map(int)
            .log("ints")
            .batch(2)
            .log("ints_pairs")
            .collect(),
            [[1, 2], [3]],
        )

        (
            Pipe.from_source("12-3".__iter__)
            .log("chars")
            .map(int)
            .log("ints", colored=True)
            .batch(2)
            .superintend(raise_if_more_errors_than=1)
        )

    def test_partial_iteration(self) -> None:
        first_elem = next(
            iter(
                Pipe.from_source(([0] * N).__iter__)
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
            Pipe.from_source(([0] * N).__iter__)
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

    def test_invalid_source(self) -> None:
        self.assertRaises(TypeError, lambda: Pipe.from_source(range(3)))  # type: ignore
        pipe_ok_at_construction: Pipe[int] = Pipe.from_source(lambda: 0)  # type: ignore
        self.assertRaises(TypeError, lambda: pipe_ok_at_construction.collect())

    @parameterized.expand([[1], [2], [3]])
    def test_invalid_flatten_upstream(self, n_threads: int):
        self.assertRaises(
            TypeError, Pipe.from_source(range(3).__iter__).flatten(n_threads=n_threads).collect  # type: ignore
        )

    def test_planning_and_execution_decoupling(self) -> None:
        a = Pipe.from_source(range(N).__iter__)
        b = a.batch(size=N)
        # test double execution
        self.assertListEqual(a.collect(), list(range(N)))
        self.assertListEqual(a.collect(), list(range(N)))
        # test b not affected by a execution
        self.assertListEqual(b.collect(), [list(range(N))])

    def test_generator_already_generating(self) -> None:
        l: List[Iterator[int]] = [
            iter((ten_ms_identity(x) for x in range(N))) for _ in range(3)
        ]
        self.assertEqual(
            Counter(Pipe.from_source(l.__iter__).flatten(n_threads=2)),
            Counter(list(range(N)) + list(range(N)) + list(range(N))),
        )

    def test_explain(self) -> None:
        p: Pipe[int] = (
            Pipe.from_source(range(8).__iter__)
            .filter(lambda _: True)
            .batch(100)
            .log("batches")
            .flatten(n_threads=4)
            .slow(64)
            .log("slowed elems")
            .chain(
                Pipe.from_source([].__iter__).do(lambda e: None).log("other 1"),
                Pipe.from_source([].__iter__).log("other 2"),
            )
            .catch(ValueError, TypeError, when=lambda e: True)
        )
        a = p.explain()
        p.collect()
        b = p.explain()
        c = p.explain(colored=True)
        self.assertEqual(a, b)
        self.assertGreater(len(c), len(a))
        print(c)

    def test_accept_typing(self) -> None:
        p: Pipe[str] = (
            Pipe.from_source(lambda: range(10))
            .batch()
            .map(lambda b: list(map(str, b)))
            .flatten()
        )
        it: Iterator[str] = iter(p)
        from kioss._visit._iter_production import IteratorProducingVisitor

        p._accept(IteratorProducingVisitor[str]())
        from kioss._visit._explanation import ExplainingVisitor

        p._accept(ExplainingVisitor())
