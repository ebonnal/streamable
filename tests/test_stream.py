import time
import timeit
import unittest
from typing import Callable, Generic, Iterable, Iterator, Set, Type, TypeVar, cast

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

# size of the test collections
N = 256
src: Callable[[], Iterable[int]] = range(N).__iter__


class TestStream(unittest.TestCase):
    def test_init(self) -> None:
        stream = Stream(src)
        self.assertEqual(
            stream.source,
            src,
            msg="The stream's `source` must be the source argument.",
        )
        self.assertIsNone(
            stream.upstream,
            msg="The `upstream` attribute of a base Stream's instance must be None.",
        )

        with self.assertRaisesRegex(
            TypeError,
            "source must be a callable but got a <class 'range'>",
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
                Stream([].__iter__).do(lambda _: None).observe("stream #2 elements"),
                Stream([].__iter__).observe("stream #3 elements"),
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
        s: Set[int] = set()
        def side_effect(x: int):
            nonlocal s
            s.add(square(x))
        res = list(Stream(src).do(side_effect, concurrency=concurrency))
        self.assertListEqual(
            res,
            list(map(square, src())),
            msg="At any concurrency the `do` method should call func on upstream elements (in any order).",
        )
        self.assertSetEqual(
            s,
            set(map(square, src())),
            msg="At any concurrency the `do` method should return the upstream elements in order.",
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
            delta=expected_iteration_duration*0.25,
            msg="Increasing the concurrency of mapping should decrease proportionnally the iteration's duration.",
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
        iterables_stream = (
            Stream(lambda: range(n_iterables))
            .map(lambda _: map(slow_identity, range(N // n_iterables)))
        )
        self.assertAlmostEqual(
            timestream(iterables_stream.flatten(concurrency=concurrency)),
            expected_iteration_duration,
            delta=expected_iteration_duration*0.25,
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
        it = list(map(slow_identity, range(N // n_iterables)))
        iterables_stream = (
            Stream(lambda: range(n_iterables))
            .map(lambda _: it)
        )
        self.assertCountEqual(
            list(iterables_stream.flatten(concurrency=concurrency)),
            list(it) * n_iterables,
            msg="At any concurrency the `flatten` method should yield all the upstream iterables' elements.",
        )

        # test potential recursion issue with chained empty iters
        list(
            Stream([iter([]) for _ in range(2000)].__iter__).flatten(
                concurrency=concurrency
            )
        )

    def test_flatten_typing(self) -> None:
        flattened_iterator_stream: Stream[str] = Stream("abc".__iter__).map(iter).flatten()
        flattened_list_stream: Stream[str] = Stream("abc".__iter__).map(list).flatten()
        flattened_set_stream: Stream[str] = Stream("abc".__iter__).map(set).flatten()
        flattened_map_stream: Stream[str] = Stream("abc".__iter__).map(lambda char: map(lambda x: x, char)).flatten()
        flattened_filter_stream: Stream[str] = Stream("abc".__iter__).map(lambda char: filter(lambda _: True, char)).flatten()

    @parameterized.expand(
        [
            [raised_exc, catched_exc, concurrency]
            for raised_exc, catched_exc in [(ValueError, ValueError), (StopIteration, RuntimeError)]
            for concurrency in [1, 2]
        ]
    )
    def test_flatten_with_exception(self, raised_exc: Type[Exception], catched_exc: Type[Exception], concurrency: int) -> None:
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
            set(Stream(lambda: range(n_iterables)).map(lambda i: cast(Iterable[int], odd_iterable(i, raised_exc))).flatten(concurrency=concurrency).catch(catched_exc)),
            set(range(0, n_iterables, 2)),
            msg="At any concurrency the `flatten` method should be resilient to exceptions thrown by iterators, especially it should remap StopIteration one to RuntimeError.",
        )
