import itertools
import time
import timeit
import unittest
from collections import Counter
from typing import Callable, Iterator, List, TypeVar

from parameterized import parameterized  # type: ignore

from iterable import Stream, _util

TEN_MS = 0.01
DELTA = 0.35
T = TypeVar("T")


def timestream(stream: Stream):
    def iterate():
        for _ in stream:
            pass

    return timeit.timeit(iterate, number=1)


# simulates an I/0 bound function
def ten_ms_identity(x: T) -> T:
    time.sleep(TEN_MS)
    return x


def raise_stopiteration() -> bool:
    raise StopIteration()


# size of the test collections
N = 64


class TestStream(unittest.TestCase):
    def test_init(self) -> None:
        # from iterable
        self.assertListEqual(list(Stream(range(8).__iter__)), list(range(8)))
        # from iterator
        self.assertListEqual(list(Stream(range(8).__iter__)), list(range(8)))

    def test_chain(self) -> None:
        # test that the order is preserved
        self.assertListEqual(
            list(
                Stream(range(2).__iter__)
                .chain(
                    Stream(range(2, 4).__iter__),
                    Stream(range(4, 6).__iter__),
                )
                .chain(Stream(range(6, 8).__iter__))
            ),
            list(range(8)),
        )

    def test_flatten_typing(self) -> None:
        a: Stream[str] = Stream("abc".__iter__).map(iter).flatten()
        b: Stream[str] = Stream("abc".__iter__).map(list).flatten()
        c: Stream[str] = Stream("abc".__iter__).map(set).flatten()

    @parameterized.expand([[1], [2], [3]])
    def test_flatten(self, n_threads: int):
        if n_threads == 1:
            # test ordering
            self.assertListEqual(
                list(
                    Stream(["Hello World", "Happy to be here :)"].__iter__)
                    .map(str.split)
                    .flatten(n_threads=n_threads)
                ),
                ["Hello", "World", "Happy", "to", "be", "here", ":)"],
            )
        self.assertSetEqual(
            set(
                Stream(["Hello World", "Happy to be here :)"].__iter__)
                .map(str.split)
                .flatten(n_threads=n_threads)
            ),
            {"Hello", "World", "Happy", "to", "be", "here", ":)"},
        )
        self.assertEqual(
            sum(
                Stream([["1 2 3", "4 5 6"], ["7", "8 9 10"]].__iter__)
                .flatten(n_threads=n_threads)
                .map(str.split)
                .flatten(n_threads=n_threads)
                .map(int)
            ),
            55,
        )

        # test potential recursion issue with chained empty iters
        list(
            Stream([iter([]) for _ in range(2000)].__iter__).flatten(n_threads=n_threads)
        )

        # test concurrency
        single_stream_iteration_duration = 0.5
        queue_get_timeout = 0.1
        streams = [
            Stream(range(0, N, 3).__iter__).slow(
                (N / 3) / single_stream_iteration_duration
            ),
            Stream(range(1, N, 3).__iter__).slow(
                (N / 3) / single_stream_iteration_duration
            ),
            Stream(range(2, N, 3).__iter__).slow(
                (N / 3) / single_stream_iteration_duration
            ),
        ]
        self.assertAlmostEqual(
            timeit.timeit(
                lambda: self.assertSetEqual(
                    set(Stream(streams.__iter__).flatten(n_threads=n_threads)),
                    set(range(N)),
                ),
                number=1,
            ),
            len(streams)
            * single_stream_iteration_duration
            / (1 if n_threads is None else n_threads),
            delta=DELTA
            * len(streams)
            * single_stream_iteration_duration
            / (1 if n_threads is None else n_threads)
            + queue_get_timeout,
        )

        # partial iteration

        zeros = lambda: Stream(([0] * N).__iter__)
        self.assertEqual(
            next(
                iter(
                    Stream([zeros(), zeros(), zeros()].__iter__).flatten(
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

        get_stream: Callable[[], Stream[int]] = lambda: (
            Stream(
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

        set(get_stream().catch(Exception, when=store_error_types))
        self.assertSetEqual(
            error_types,
            {ValueError, TypeError, AssertionError, RuntimeError},
        )
        self.assertSetEqual(
            set(get_stream().catch(Exception)),
            set(range(7)),
        )

        # test rasing:
        self.assertRaises(
            ValueError,
            lambda: list(Stream([map(int, "12-3")].__iter__).flatten(n_threads=n_threads)),  # type: ignore
        )
        self.assertRaises(
            ValueError,
            lambda: list(Stream(lambda: map(int, "-")).flatten(n_threads=n_threads)),  # type: ignore
        )

    def test_add(self) -> None:
        self.assertListEqual(
            list(
                sum(
                    [
                        Stream(range(0, 2).__iter__),
                        Stream(range(2, 4).__iter__),
                        Stream(range(4, 6).__iter__),
                        Stream(range(6, 8).__iter__),
                    ],
                    start=Stream([].__iter__),
                )
            ),
            list(range(8)),
        )

    @parameterized.expand([[1], [2], [3]])
    def test_map(self, n_threads: int):
        func = lambda x: x**2
        self.assertSetEqual(
            set(
                Stream(range(N).__iter__)
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
                Stream(l.__iter__)
                .map(lambda l: iter(l))
                .map(next, n_threads=n_threads)
                .catch(RuntimeError)
            ),
            {1, 3},
        )

    def test_map_threading_bench(self) -> None:
        # non-threaded vs threaded execution time
        stream = Stream(range(N).__iter__).map(ten_ms_identity)
        self.assertAlmostEqual(timestream(stream), TEN_MS * N, delta=DELTA * (TEN_MS * N))
        n_threads = 2
        stream = Stream(range(N).__iter__).map(ten_ms_identity, n_threads=n_threads)
        self.assertAlmostEqual(
            timestream(stream),
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
            list(Stream(args.__iter__).do(func_with_side_effect)),
            list(args),
        )
        self.assertListEqual(l, list(map(func, args)))

        # with threads
        l.clear()
        self.assertSetEqual(
            set(Stream(args.__iter__).do(func_with_side_effect, n_threads=2)),
            set(args),
        )
        self.assertSetEqual(set(l), set(map(func, args)))

        self.assertEqual(
            list(
                Stream(range(N).__iter__)
                .do(lambda n: None if n % 2 == 0 else raise_stopiteration())
                .catch(RuntimeError)
            ),
            list(range(0, N, 2)),
        )

    def test_filter(self) -> None:
        self.assertListEqual(
            list(Stream(range(8).__iter__).filter(lambda x: x % 2 != 0)),
            [1, 3, 5, 7],
        )

        self.assertListEqual(list(Stream(range(8).__iter__).filter(lambda _: False)), [])

        self.assertEqual(
            list(
                Stream(range(N).__iter__)
                .filter(lambda n: True if n % 2 == 0 else raise_stopiteration())
                .catch(RuntimeError)
            ),
            list(range(0, N, 2)),
        )

    def test_batch(self) -> None:
        self.assertListEqual(
            list(Stream(range(8).__iter__).batch(size=3)),
            [[0, 1, 2], [3, 4, 5], [6, 7]],
        )
        self.assertListEqual(
            list(Stream(range(6).__iter__).batch(size=3)),
            [[0, 1, 2], [3, 4, 5]],
        )
        self.assertListEqual(
            list(Stream(range(8).__iter__).batch(size=1)),
            list(map(lambda x: [x], range(8))),
        )
        self.assertListEqual(
            list(Stream(range(8).__iter__).batch(size=8)),
            [list(range(8))],
        )
        self.assertEqual(
            len(list(Stream(range(8).__iter__).slow(10).batch(period=0.09))),
            7,
        )
        # assert batch gracefully yields if next elem throw exception
        self.assertListEqual(
            list(Stream("01234-56789".__iter__).map(int).batch(2).catch(ValueError)),
            [[0, 1], [2, 3], [4], [5, 6], [7, 8], [9]],
        )
        self.assertListEqual(
            list(Stream("0123-56789".__iter__).map(int).batch(2).catch(ValueError)),
            [[0, 1], [2, 3], [5, 6], [7, 8], [9]],
        )
        errors = set()

        def store_errors(error):
            errors.add(error)
            return True

        self.assertListEqual(
            list(
                Stream("0123-56789".__iter__)
                .map(int)
                .batch(2)
                .catch(ValueError, when=store_errors)
                .flatten()
                .map(type)
            ),
            [int, int, int, int, int, int, int, int, int],
        )
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(next(iter(errors)), ValueError)

    @parameterized.expand([[1], [2], [3]])
    def test_slow(self, n_threads: int):
        freq = 64
        stream = (
            Stream(range(N).__iter__).map(ten_ms_identity, n_threads=n_threads).slow(freq)
        )
        self.assertAlmostEqual(
            timestream(stream),
            1 / freq * N,
            delta=DELTA * (1 / freq * N),
        )

    def test_time(self) -> None:
        new_stream = lambda: Stream(range(8).__iter__).slow(64)
        start_time = time.time()
        list(new_stream())
        execution_time = time.time() - start_time
        self.assertAlmostEqual(
            execution_time, timestream(new_stream()), delta=DELTA * execution_time
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
                Stream(["1", "r", "2"].__iter__)
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
            lambda: list(
                Stream(["1", "r", "2"].__iter__)
                .map(int, n_threads=n_threads)
                .catch(ValueError, when=lambda error: False)
            ),
        )
        self.assertListEqual(
            list(
                Stream(["1", "r", "2"].__iter__)
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
                Stream(["1", "r", "2"].__iter__)
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
            lambda: list(
                Stream(["1", "r", "2"].__iter__)
                .map(int, n_threads=n_threads)
                .catch(TypeError)
                .map(type)
            ),
        )
        self.assertRaises(
            ValueError,
            lambda: list(
                Stream(["1", "r", "2"].__iter__)
                .map(int, n_threads=n_threads)
                .catch(TypeError)
                .map(type)
            ),
        )

    def test_iterate(self) -> None:
        self.assertListEqual(Stream("123".__iter__).map(int).iterate(collect_limit=2), [1, 2])
        self.assertListEqual(Stream("123".__iter__).map(int).iterate(), [])

        # errors
        iterate = Stream("12-3".__iter__).map(int).iterate
        self.assertRaises(
            ValueError,
            iterate,
        )
        # does not raise with sufficient threshold
        iterate(raise_if_more_errors_than=1)
        # raise with insufficient threshold
        self.assertRaises(
            ValueError,
            lambda: iterate(raise_if_more_errors_than=0),
        )

        # fail_fast
        self.assertRaises(
            ValueError,
            lambda: Stream("a-b".__iter__).map(int).iterate(fail_fast=True),
        )

    def test_log(self) -> None:
        self.assertListEqual(
            list(
                Stream("123".__iter__)
                .observe("chars")
                .map(int)
                .observe("ints")
                .batch(2)
                .observe("ints_pairs")
            ),
            [[1, 2], [3]],
        )

        (
            Stream("12-3".__iter__)
            .observe("chars")
            .map(int)
            .observe("ints", colored=True)
            .batch(2)
            .iterate(raise_if_more_errors_than=1)
        )

    def test_partial_iteration(self) -> None:
        first_elem = next(
            iter(
                Stream(([0] * N).__iter__)
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
        stream = (
            Stream(([0] * N).__iter__)
            .slow(50)
            .map(_util.identity, n_threads=2)
            .slow(50)
            .map(_util.identity, n_threads=2)
            .slow(50)
            .map(_util.identity, n_threads=2)
            .slow(50)
        )
        samples = list(itertools.islice(stream, n))
        self.assertListEqual(samples, [0] * n)

    def test_invalid_source(self) -> None:
        self.assertRaises(TypeError, lambda: Stream(range(3)))  # type: ignore
        stream_ok_at_construction: Stream[int] = Stream(lambda: 0)  # type: ignore
        self.assertRaises(TypeError, lambda: list(stream_ok_at_construction))

    @parameterized.expand([[1], [2], [3]])
    def test_invalid_flatten_upstream(self, n_threads: int):
        self.assertRaises(
            TypeError, lambda: list(Stream(range(3).__iter__).flatten(n_threads=n_threads))  # type: ignore
        )

    def test_planning_and_execution_decoupling(self) -> None:
        a = Stream(range(N).__iter__)
        b = a.batch(size=N)
        # test double execution
        self.assertListEqual(list(a), list(range(N)))
        self.assertListEqual(list(a), list(range(N)))
        # test b not affected by a execution
        self.assertListEqual(list(b), [list(range(N))])

    def test_generator_already_generating(self) -> None:
        l: List[Iterator[int]] = [
            iter((ten_ms_identity(x) for x in range(N))) for _ in range(3)
        ]
        self.assertEqual(
            Counter(Stream(l.__iter__).flatten(n_threads=2)),
            Counter(list(range(N)) + list(range(N)) + list(range(N))),
        )

    def test_explain(self) -> None:
        p: Stream[int] = (
            Stream(range(8).__iter__)
            .filter(lambda _: True)
            .map(lambda x: x)
            .batch(100)
            .observe("batches")
            .flatten(n_threads=4)
            .slow(64)
            .observe("slowed elems")
            .chain(
                Stream([].__iter__).do(lambda e: None).observe("other 1"),
                Stream([].__iter__).observe("other 2"),
            )
            .catch(ValueError, TypeError, when=lambda e: True)
        )
        a = p.explain()
        list(p)
        b = p.explain()
        c = p.explain(colored=True)
        self.assertEqual(a, b)
        self.assertGreater(len(c), len(a))
        print(c)

    def test_accept_typing(self) -> None:
        p: Stream[str] = (
            Stream(lambda: range(10)).batch().map(lambda b: list(map(str, b))).flatten()
        )
        it: Iterator[str] = iter(p)
        from iterable._visit._iter import IteratorProducingVisitor

        p._accept(IteratorProducingVisitor[str]())
        from iterable._visit._explanation import ExplainingVisitor

        p._accept(ExplainingVisitor())
