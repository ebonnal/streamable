import time
import timeit
import unittest
from typing import Any, Callable, Iterable, Iterator, List, Set, Type, TypeVar, cast

from parameterized import parameterized  # type: ignore

from streamable import Stream

T = TypeVar("T")


def timestream(stream: Stream):
    def iterate():
        for _ in stream:
            pass

    return timeit.timeit(iterate, number=1)


# simulates an I/0 bound function
slow_identity_duration = 0.01


def slow_identity(x: T) -> T:
    time.sleep(slow_identity_duration)
    return x


def identity(x: T) -> T:
    return x


def square(x):
    return x**2


def throw(exc: Type[Exception]):
    raise exc()


class TestError(Exception):
    pass


DELTA_RATE = 0.25
# size of the test collections
N = 256


def src() -> Iterable[int]:
    return range(N)


def pair_src() -> Iterable[int]:
    return range(0, N, 2)


class TestStream(unittest.TestCase):
    def test_init(self) -> None:
        stream = Stream(src)
        self.assertEqual(
            stream._source,
            src,
            msg="The stream's `source` must be the source argument.",
        )
        self.assertIsNone(
            stream.upstream(),
            msg="The `upstream` attribute of a base Stream's instance must be None.",
        )

        with self.assertRaisesRegex(
            TypeError,
            "`source` must be a callable but got a <class 'range'>",
            msg="Instantiating a Stream with a source not being a callable must raise TypeError.",
        ):
            Stream(range(N))  # type: ignore

    def test_explain(self) -> None:
        complex_stream: Stream[int] = (
            Stream(src)
            .filter(lambda _: True)
            .map(lambda _: _)
            .batch(100)
            .observe("batches")
            .flatten(concurrency=4)
            .slow(64)
            .observe("stream #1 elements")
            .chain(
                Stream(lambda: []).do(lambda _: None).observe("stream #2 elements"),
                Stream(lambda: []).observe("stream #3 elements"),
            )
            .catch(ValueError, TypeError, when=lambda _: True)
        )
        explanation_1 = complex_stream.explain()
        explanation_2 = complex_stream.explain()
        self.assertEqual(
            explanation_1,
            explanation_2,
            msg="Stream.explain() must be deterministic.",
        )
        colored_explanation = complex_stream.explain(colored=True)
        self.assertNotEqual(
            explanation_1,
            colored_explanation,
            msg="Stream.explain(colored=True) must different from non colored one.",
        )
        explanation_3 = complex_stream.map(str).explain()
        self.assertNotEqual(
            explanation_1,
            explanation_3,
            msg="explanation of different streams must be different",
        )

        print(colored_explanation)

    def test_iter(self) -> None:
        self.assertIsInstance(
            iter(Stream(src)),
            Iterator,
            msg="iter(stream) must return an Iterator.",
        )

    def test_add(self) -> None:
        from streamable._stream import ChainStream

        stream = Stream(src)
        self.assertIsInstance(
            stream + stream,
            ChainStream,
            msg="stream addition must return a ChainStream.",
        )

    @parameterized.expand(
        [
            [Stream.map, [identity]],
            [Stream.do, [identity]],
            [Stream.flatten, []],
        ]
    )
    def test_sanitize_concurrency(self, method, args) -> None:
        stream = Stream(src)
        with self.assertRaises(
            TypeError,
            msg=f"{method} should be raising TypeError for non-int concurrency.",
        ):
            method(stream, *args, concurrency="1")

        with self.assertRaises(
            ValueError, msg=f"{method} should be raising ValueError for concurrency=0."
        ):
            method(stream, *args, concurrency=0)

        for concurrency in range(1, 10):
            self.assertIsInstance(
                method(stream, *args, concurrency=concurrency),
                Stream,
                msg=f"It must be ok to call {method} with concurrency={concurrency}.",
            )

    @parameterized.expand(
        [
            [1],
            [2],
        ]
    )
    def test_map(self, concurrency) -> None:
        self.assertListEqual(
            list(Stream(src).map(square, concurrency=concurrency)),
            list(map(square, src())),
            msg="At any concurrency the `map` method should act as the builtin map function, transforming elements while preserving input elements order.",
        )

    @parameterized.expand(
        [
            [1],
            [2],
        ]
    )
    def test_do(self, concurrency) -> None:
        side_collection: Set[int] = set()

        def side_effect(x: int, func: Callable[[int], int]):
            nonlocal side_collection
            side_collection.add(func(x))

        res = list(
            Stream(src).do(lambda i: side_effect(i, square), concurrency=concurrency)
        )

        self.assertListEqual(
            res,
            list(src()),
            msg="At any concurrency the `do` method should return the upstream elements in order.",
        )
        self.assertSetEqual(
            side_collection,
            set(map(square, src())),
            msg="At any concurrency the `do` method should call func on upstream elements (in any order).",
        )

    @parameterized.expand(
        [
            [raised_exc, catched_exc, concurrency, method]
            for raised_exc, catched_exc in [
                (TestError, TestError),
                (StopIteration, RuntimeError),
            ]
            for concurrency in [1, 2]
            for method in [Stream.do, Stream.map]
        ]
    )
    def test_map_or_do_with_exception(
        self,
        raised_exc: Type[Exception],
        catched_exc: Type[Exception],
        concurrency: int,
        method: Callable[[Stream, Callable[[Any], int], int], Stream],
    ) -> None:
        with self.assertRaises(
            catched_exc,
            msg="At any concurrency, `map`and `do` must raise",
        ):
            list(method(Stream(src), lambda _: throw(raised_exc), concurrency))

        self.assertListEqual(
            list(
                method(
                    Stream(src),
                    lambda i: throw(raised_exc) if i % 2 == 1 else i,
                    concurrency,
                ).catch(catched_exc)
            ),
            list(pair_src()),
            msg="At any concurrency, `map`and `do` must not stop after one exception occured.",
        )

    @parameterized.expand(
        [
            [method, concurrency]
            for method in [Stream.do, Stream.map]
            for concurrency in [1, 2, 4]
        ]
    )
    def test_map_and_do_concurrency(self, method, concurrency) -> None:
        expected_iteration_duration = N * slow_identity_duration / concurrency
        self.assertAlmostEqual(
            timestream(method(Stream(src), slow_identity, concurrency=concurrency)),
            expected_iteration_duration,
            delta=expected_iteration_duration * DELTA_RATE,
            msg="Increasing the concurrency of mapping should decrease proportionnally the iteration's duration.",
        )

    @parameterized.expand(
        [
            [1],
            [2],
        ]
    )
    def test_flatten(self, concurrency) -> None:
        n_iterables = 32
        it = list(range(N // n_iterables))
        double_it = it + it
        iterables_stream = Stream(
            lambda: map(slow_identity, [double_it] + [it for _ in range(n_iterables)])
        )
        self.assertCountEqual(
            list(iterables_stream.flatten(concurrency=concurrency)),
            list(it) * n_iterables + double_it,
            msg="At any concurrency the `flatten` method should yield all the upstream iterables' elements.",
        )
        self.assertListEqual(
            list(
                Stream(lambda: [iter([]) for _ in range(2000)]).flatten(
                    concurrency=concurrency
                )
            ),
            [],
            msg="`flatten` should not yield any element if upstream elements are empty iterables, and be resilient to recursion issue in case of successive empty upstream iterables.",
        )

        with self.assertRaises(
            TypeError,
            msg="`flatten` should raise if an upstream element is not iterable.",
        ):
            next(iter(Stream(cast(Callable[[], Iterable], src)).flatten()))

    def test_flatten_typing(self) -> None:
        flattened_iterator_stream: Stream[str] = (
            Stream(lambda: "abc").map(iter).flatten()
        )
        flattened_list_stream: Stream[str] = Stream(lambda: "abc").map(list).flatten()
        flattened_set_stream: Stream[str] = Stream(lambda: "abc").map(set).flatten()
        flattened_map_stream: Stream[str] = (
            Stream(lambda: "abc").map(lambda char: map(lambda x: x, char)).flatten()
        )
        flattened_filter_stream: Stream[str] = (
            Stream(lambda: "abc")
            .map(lambda char: filter(lambda _: True, char))
            .flatten()
        )

    @parameterized.expand(
        [
            [1],
            [2],
            [4],
        ]
    )
    def test_flatten_concurrency(self, concurrency) -> None:
        expected_iteration_duration = N * slow_identity_duration / concurrency
        n_iterables = 32
        iterables_stream = Stream(lambda: range(n_iterables)).map(
            lambda _: map(slow_identity, range(N // n_iterables))
        )
        self.assertAlmostEqual(
            timestream(iterables_stream.flatten(concurrency=concurrency)),
            expected_iteration_duration,
            delta=expected_iteration_duration * DELTA_RATE,
            msg="Increasing the concurrency of mapping should decrease proportionnally the iteration's duration.",
        )

    @parameterized.expand(
        [
            [raised_exc, catched_exc, concurrency]
            for raised_exc, catched_exc in [
                (TestError, TestError),
                (StopIteration, RuntimeError),
            ]
            for concurrency in [1, 2]
        ]
    )
    def test_flatten_with_exception(
        self,
        raised_exc: Type[Exception],
        catched_exc: Type[Exception],
        concurrency: int,
    ) -> None:
        class odd_iterable(Iterable[int]):
            def __init__(self, i, pair_exception: Type[Exception]):
                self.i = i
                self.pair_exception = pair_exception

            def __iter__(self) -> Iterator[int]:
                if self.i % 2:
                    raise self.pair_exception()
                yield self.i

        n_iterables = 4

        self.assertSetEqual(
            set(
                Stream(lambda: range(n_iterables))
                .map(lambda i: cast(Iterable[int], odd_iterable(i, raised_exc)))
                .flatten(concurrency=concurrency)
                .catch(catched_exc)
            ),
            set(range(0, n_iterables, 2)),
            msg="At any concurrency the `flatten` method should be resilient to exceptions thrown by iterators, especially it should remap StopIteration one to RuntimeError.",
        )

    @parameterized.expand([[concurrency] for concurrency in [2, 4]])
    def test_partial_iteration_on_streams_using_concurrency(
        self, concurrency: int
    ) -> None:
        from streamable.functions._itertools import _CONCURRENCY_BUFFER_SIZE_FACTOR

        yielded_elems = []

        def remembering_src() -> Iterator[int]:
            nonlocal yielded_elems
            for elem in src():
                yielded_elems.append(elem)
                yield elem

        for stream in [
            Stream(remembering_src).map(identity, concurrency=concurrency),
            Stream(remembering_src).do(identity, concurrency=concurrency),
            Stream(remembering_src).batch(1).flatten(concurrency=concurrency),
        ]:
            yielded_elems = []
            next(iter(stream))
            self.assertEqual(
                len(yielded_elems),
                concurrency * _CONCURRENCY_BUFFER_SIZE_FACTOR,
                msg=f"after only one `next` a concurrent {type(stream)} should have pulled only concurrency * BUFFER_SIZE_FACTOR={concurrency * _CONCURRENCY_BUFFER_SIZE_FACTOR} upstream elements.",
            )

    def test_chain(self) -> None:
        stream_a = Stream(lambda: range(10))
        stream_b = Stream(lambda: range(10, 20))
        stream_c = Stream(lambda: range(20, 30))
        self.assertListEqual(
            list(stream_a.chain(stream_b, stream_c)),
            list(range(30)),
            msg="`chain` must yield the elements of the first stream the move on with the elements of the next ones and so on.",
        )

    def test_filter(self) -> None:
        def predicate(x) -> bool:
            return x % 2 == 1

        self.assertListEqual(
            list(Stream(src).filter(predicate)),
            list(filter(predicate, src())),
            msg="`filter` must act like builtin filter",
        )

    def test_batch(self) -> None:
        # behavior with invalid arguments
        for seconds in [-1, 0]:
            with self.assertRaises(
                ValueError,
                msg="`batch` should raise error when called with `seconds` <= 0.",
            ):
                list(Stream(lambda: [1]).batch(size=100, seconds=seconds)),
        for size in [-1, 0]:
            with self.assertRaises(
                ValueError,
                msg="`batch` should raise error when called with `size` < 1.",
            ):
                list(Stream(lambda: [1]).batch(size=size)),

        # batch size
        self.assertListEqual(
            list(Stream(lambda: range(6)).batch(size=4)),
            [[0, 1, 2, 3], [4, 5]],
            msg="",
        )
        self.assertListEqual(
            list(Stream(lambda: range(6)).batch(size=2)),
            [[0, 1], [2, 3], [4, 5]],
            msg="",
        )
        self.assertListEqual(
            list(Stream(lambda: []).batch(size=2)),
            [],
            msg="",
        )

        # behavior with exceptions
        def f(i):
            return i / (10 - i)

        stream_iterator = iter(Stream(lambda: map(f, src())).batch(100))
        self.assertListEqual(
            next(stream_iterator),
            list(map(f, range(10))),
            msg="when encountering upstream exception, `batch` should yield the current accumulated batch...",
        )

        with self.assertRaises(
            ZeroDivisionError,
            msg="... and raise the upstream exception during the next call to `next`...",
        ):
            next(stream_iterator)

        self.assertListEqual(
            next(stream_iterator),
            list(map(f, range(11, 111))),
            msg="... and restarting a fresh batch to yield after that.",
        )

        # behavior of the `seconds` parameter
        self.assertListEqual(
            list(
                Stream(lambda: map(slow_identity, src())).batch(
                    size=100, seconds=0.9 * slow_identity_duration
                )
            ),
            list(map(lambda e: [e], src())),
            msg="`batch` should yield each upstream element alone in a single-element batch if `seconds` inferior to the upstream yield period",
        )
        self.assertListEqual(
            list(
                Stream(lambda: map(slow_identity, src())).batch(
                    size=100, seconds=1.8 * slow_identity_duration
                )
            ),
            list(map(lambda e: [e, e + 1], pair_src())),
            msg="`batch` should yield upstream elements in a two-element batch if `seconds` inferior to twice the upstream yield period",
        )

    def test_slow(self) -> None:
        # behavior with invalid arguments
        for frequency in [-0.01, 0]:
            with self.assertRaises(
                ValueError,
                msg="`slow` should raise error when called with `frequency` <= 0.",
            ):
                list(Stream(lambda: [1]).slow(frequency=frequency))

        # float or int frequency
        for frequency in [0.9, 2]:
            stream_iterator = iter(Stream(src).slow(frequency=frequency))
            start_time = time.time()
            a = next(stream_iterator)
            b = next(stream_iterator)
            end_time = time.time()
            self.assertEqual(
                a, 0, msg="`slow` must forward upstream elements unchanged"
            )
            self.assertEqual(
                b, 1, msg="`slow` must forward upstream elements unchanged"
            )
            expected_duration = 1 / frequency
            self.assertAlmostEqual(
                end_time - start_time,
                expected_duration,
                delta=0.01 * expected_duration,
                msg="`slow` must respect the frequency set.",
            )

    def test_catch(self) -> None:
        def f(i):
            return i / (3 - i)

        stream = Stream(lambda: map(f, src()))
        safe_src = list(src())
        del safe_src[3]
        self.assertListEqual(
            list(stream.catch(ZeroDivisionError)),
            list(map(f, safe_src)),
            msg="If the exception type matches the `catch`'s argument, then the impacted element should be .",
        )
        self.assertListEqual(
            list(stream.catch(TestError, ZeroDivisionError)),
            list(map(f, safe_src)),
            msg="If the exception type matches one of the `catch`'s argument, then the impacted element should be .",
        )
        self.assertListEqual(
            list(stream.catch(TestError, ZeroDivisionError, when=lambda _: True)),
            list(map(f, safe_src)),
            msg="If the exception type matches one of the `catch`'s argument and `when`predicate evaluates to True, then the impacted element should be .",
        )

        with self.assertRaises(
            ZeroDivisionError,
            msg="If a non catched exception type occurs, then it should be raised.",
        ):
            list(stream.catch(TestError))
        with self.assertRaises(
            ZeroDivisionError,
            msg="If an exception of a catched type occurs but predicate `when` does not return True, then it should be raised.",
        ):
            list(stream.catch(ZeroDivisionError, when=lambda _: False))

        first_value = 1
        second_value = 2
        third_value = 3
        functions = [
            lambda: throw(TestError),
            lambda: throw(TypeError),
            lambda: first_value,
            lambda: second_value,
            lambda: throw(ValueError),
            lambda: third_value,
            lambda: throw(ZeroDivisionError),
        ]

        erroring_stream: Stream[int] = Stream(
            lambda: map(lambda f: f(), functions)
        ).catch(Exception, raise_at_exhaustion=True)
        erroring_stream_iterator = iter(erroring_stream)
        self.assertEqual(
            next(erroring_stream_iterator),
            first_value,
            msg="`catch` should yield the first non exception throwing element.",
        )
        yields_count = 1
        with self.assertRaises(
            TestError,
            msg="`catch` should raise the first error encountered when `raise_at_exhaustion` is True.",
        ):
            for _ in erroring_stream_iterator:
                yields_count += 1
        with self.assertRaises(
            StopIteration,
            msg="`catch` with `raise_at_exhaustion`=True should finally raise StopIteration to avoid infinite recursion if there is another catch downstream.",
        ):
            next(erroring_stream_iterator)
        self.assertEqual(
            yields_count,
            3,
            msg="3 elements should have passed been yielded between catched exceptions.",
        )

        only_catched_errors_stream = (
            Stream(lambda: range(2000)).map(lambda i: throw(TestError)).catch(TestError)
        )
        self.assertEqual(
            list(only_catched_errors_stream),
            [],
            msg="When upstream raise exceptions without yielding any element, listing the stream must return empty list, without recursion issue.",
        )
        with self.assertRaises(
            StopIteration,
            msg="When upstream raise exceptions without yielding any element, then the first call to `next` on a stream catching all errors should raise StopIteration.",
        ):
            next(iter(only_catched_errors_stream))

    def test_observe(self) -> None:
        self.assertListEqual(
            list(
                Stream(lambda: "123-567")
                .observe("chars")
                .map(int)
                .observe("ints", colored=True)
                .batch(2)
                .observe("ints_pairs")
                .catch(ValueError)
            ),
            [[1, 2], [3], [5, 6], [7]],
            msg="This can break due to `batch`/`map`/`catch`, check other breaking tests to determine quickly if it's an issue with `observe`.",
        )

    def test_is_iterable(self) -> None:
        self.assertIsInstance(Stream(src), Iterable)

    def test_exhaust(self) -> None:
        l: List[int] = []

        def effect(x: int) -> None:
            nonlocal l
            l.append(x)

        self.assertEqual(
            Stream(src).do(effect).exhaust(),
            N,
            msg="`exhaust` should return the number of iterated elements.",
        )
        self.assertListEqual(
            l, list(src()), msg="`exhaust` should iterate over the entire stream."
        )

        Stream(src).do(effect).exhaust(explain=True)

    def test_multiple_iterations(self):
        stream = Stream(lambda: map(identity, src()))
        for _ in range(3):
            self.assertEqual(
                list(stream),
                list(src()),
                msg="The first iteration over a stream should yield the same elements as any subsequent iteration on the same stream, even if it is based on a `source` returning an iterator that only support 1 iteration.",
            )
