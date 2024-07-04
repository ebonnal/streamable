import asyncio
import time
import timeit
import unittest
from typing import (
    Any,
    Callable,
    Coroutine,
    Iterable,
    Iterator,
    List,
    Set,
    Tuple,
    Type,
    TypeVar,
    cast,
)

from parameterized import parameterized  # type: ignore

from streamable import Stream
from streamable.functions import NoopStopIteration

T = TypeVar("T")


def timestream(stream: Stream[T]) -> Tuple[float, List[T]]:
    res: List[T] = []

    def iterate():
        nonlocal res
        res = list(stream)

    return timeit.timeit(iterate, number=1), res


# simulates an I/0 bound function
slow_identity_duration = 0.01


def slow_identity(x: T) -> T:
    time.sleep(slow_identity_duration)
    return x


async def async_slow_identity(x: T) -> T:
    await asyncio.sleep(slow_identity_duration)
    return x


def identity(x: T) -> T:
    return x


# fmt: off
async def async_identity(x: T) -> T: return x
# fmt: on


def square(x):
    return x**2


async def async_square(x):
    return x**2


def throw(exc: Type[Exception]):
    raise exc()


def throw_func(exc: Type[Exception]) -> Callable[[Any], None]:
    return lambda _: throw(exc)


def async_throw_func(exc: Type[Exception]) -> Callable[[Any], Coroutine]:
    async def f(_: Any) -> None:
        raise exc

    return f


def throw_for_odd_func(exc):
    return lambda i: throw(exc) if i % 2 == 1 else i


def async_throw_for_odd_func(exc):
    async def f(i):
        return throw(exc) if i % 2 == 1 else i

    return f


class TestError(Exception):
    pass


DELTA_RATE = 0.4
# size of the test collections
N = 256

src = range(N)

pair_src = range(0, N, 2)


def less_and_less_slow_src() -> Iterator[int]:
    """
    Same as `src` but each element is yielded after a sleep time that gets shorter and shorter.
    """
    for i, elem in enumerate(src):
        time.sleep(0.1 / (i + 1))
        yield elem


def range_raising_at_exhaustion(
    start: int, end: int, step: int, exception: Exception
) -> Iterator[int]:
    yield from range(start, end, step)
    raise exception


src_raising_at_exhaustion = lambda: range_raising_at_exhaustion(0, N, 1, TestError())


class TestStream(unittest.TestCase):
    def test_init(self) -> None:
        stream = Stream(src)
        self.assertEqual(
            stream._source,
            src,
            msg="The stream's `source` must be the source argument.",
        )
        self.assertIsNone(
            stream.upstream,
            msg="The `upstream` attribute of a base Stream's instance must be None.",
        )

        with self.assertRaisesRegex(
            TypeError,
            "`source` must be either a Callable\[\[\], Iterable\] or an Iterable, but got a <class 'int'>",
            msg="Getting an Iterator from a Stream with a source not being a Union[Callable[[], Iterator], ITerable] must raise TypeError.",
        ):
            Stream(1)  # type: ignore

        self.assertIs(
            Stream(src)
            .group(100)
            .flatten()
            .map(identity)
            .amap(async_identity)
            .filter()
            .foreach(identity)
            .aforeach(async_identity)
            .catch()
            .observe()
            .slow(1)
            .source,
            src,
            msg="`source` must be propagated by operations",
        )

        with self.assertRaises(
            AttributeError,
            msg="attribute `source` must be read-only",
        ):
            Stream(src).source = src  # type: ignore

        with self.assertRaises(
            AttributeError,
            msg="attribute `upstream` must be read-only",
        ):
            Stream(src).upstream = Stream(src)  # type: ignore

    def test_explanation(self) -> None:
        class CustomCallable:
            pass

        complex_stream: Stream[int] = (
            Stream(src)
            .truncate(1024, when=lambda _: False)
            .filter()
            .foreach(lambda _: _)
            .aforeach(async_identity)
            .map(cast(Callable[[Any], Any], CustomCallable()))
            .amap(async_identity)
            .group(100)
            .observe("groups")
            .flatten(concurrency=4)
            .slow(64)
            .observe("stream #1 elements")
            .catch(TypeError, finally_raise=True)
        )
        explanation_1 = complex_stream.explanation()
        explanation_2 = complex_stream.explanation()
        self.assertEqual(
            explanation_1,
            explanation_2,
            msg="Stream.explain() must be deterministic.",
        )

        explanation_2 = complex_stream.map(str).explanation()
        self.assertNotEqual(
            explanation_1,
            explanation_2,
            msg="explanation of different streams must be different",
        )

        print(explanation_1)

    def test_explain(self) -> None:
        stream = Stream(src)
        self.assertEqual(
            stream.explain(),
            stream,
            msg="explain` should return self",
        )

    def test_iter(self) -> None:
        self.assertIsInstance(
            iter(Stream(src)),
            Iterator,
            msg="iter(stream) must return an Iterator.",
        )

    def test_add(self) -> None:
        from streamable.stream import FlattenStream

        stream = Stream(src)
        self.assertIsInstance(
            stream + stream,
            FlattenStream,
            msg="stream addition must return a FlattenStream.",
        )

        stream_a = Stream(range(10))
        stream_b = Stream(range(10, 20))
        stream_c = Stream(range(20, 30))
        self.assertListEqual(
            list(stream_a + stream_b + stream_c),
            list(range(30)),
            msg="`chain` must yield the elements of the first stream the move on with the elements of the next ones and so on.",
        )

    @parameterized.expand(
        [
            [Stream.map, [identity]],
            [Stream.amap, [async_identity]],
            [Stream.foreach, [identity]],
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
            list(Stream(less_and_less_slow_src).map(square, concurrency=concurrency)),
            list(map(square, src)),
            msg="At any concurrency the `map` method should act as the builtin map function, transforming elements while preserving input elements order.",
        )

    @parameterized.expand(
        [
            [1],
            [2],
        ]
    )
    def test_foreach(self, concurrency) -> None:
        side_collection: Set[int] = set()

        def side_effect(x: int, func: Callable[[int], int]):
            nonlocal side_collection
            side_collection.add(func(x))

        res = list(
            Stream(less_and_less_slow_src).foreach(
                lambda i: side_effect(i, square), concurrency=concurrency
            )
        )

        self.assertListEqual(
            res,
            list(src),
            msg="At any concurrency the `foreach` method should return the upstream elements in order.",
        )
        self.assertSetEqual(
            side_collection,
            set(map(square, src)),
            msg="At any concurrency the `foreach` method should call func on upstream elements (in any order).",
        )

    @parameterized.expand(
        [
            [
                raised_exc,
                catched_exc,
                concurrency,
                method,
                throw_func_,
                throw_for_odd_func_,
            ]
            for raised_exc, catched_exc in [
                (TestError, TestError),
                (StopIteration, (NoopStopIteration, RuntimeError)),
            ]
            for concurrency in [1, 2]
            for method, throw_func_, throw_for_odd_func_ in [
                (Stream.foreach, throw_func, throw_for_odd_func),
                (Stream.map, throw_func, throw_for_odd_func),
                (Stream.amap, async_throw_func, async_throw_for_odd_func),
            ]
        ]
    )
    def test_map_or_foreach_with_exception(
        self,
        raised_exc: Type[Exception],
        catched_exc: Type[Exception],
        concurrency: int,
        method: Callable[[Stream, Callable[[Any], int], int], Stream],
        throw_func: Callable[[Exception], Callable[[Any], int]],
        throw_for_odd_func: Callable[[Exception], Callable[[Any], int]],
    ) -> None:
        with self.assertRaises(
            catched_exc,
            msg="At any concurrency, `map`and `foreach` must raise",
        ):
            list(method(Stream(src), throw_func(raised_exc), concurrency))  # type: ignore

        self.assertListEqual(
            list(
                method(Stream(src), throw_for_odd_func(raised_exc), concurrency).catch(catched_exc)  # type: ignore
            ),
            list(pair_src),
            msg="At any concurrency, `map`and `foreach` must not stop after one exception occured.",
        )

    @parameterized.expand(
        [
            [method, func, concurrency]
            for method, func in [
                (Stream.foreach, slow_identity),
                (Stream.map, slow_identity),
                (Stream.amap, async_slow_identity),
            ]
            for concurrency in [1, 2, 4]
        ]
    )
    def test_map_and_foreach_concurrency(self, method, func, concurrency) -> None:
        expected_iteration_duration = N * slow_identity_duration / concurrency
        duration, res = timestream(method(Stream(src), func, concurrency=concurrency))
        self.assertListEqual(res, list(src))
        self.assertAlmostEqual(
            duration,
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
                Stream([iter([]) for _ in range(2000)]).flatten(concurrency=concurrency)
            ),
            [],
            msg="`flatten` should not yield any element if upstream elements are empty iterables, and be resilient to recursion issue in case of successive empty upstream iterables.",
        )

        with self.assertRaises(
            TypeError,
            msg="`flatten` should raise if an upstream element is not iterable.",
        ):
            next(iter(Stream(cast(Iterable, src)).flatten()))

    def test_flatten_typing(self) -> None:
        flattened_iterator_stream: Stream[str] = Stream("abc").map(iter).flatten()
        flattened_list_stream: Stream[str] = Stream("abc").map(list).flatten()
        flattened_set_stream: Stream[str] = Stream("abc").map(set).flatten()
        flattened_map_stream: Stream[str] = (
            Stream("abc").map(lambda char: map(lambda x: x, char)).flatten()
        )
        flattened_filter_stream: Stream[str] = (
            Stream("abc").map(lambda char: filter(lambda _: True, char)).flatten()
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
        iterables_stream = Stream(range(n_iterables)).map(
            lambda _: map(slow_identity, range(N // n_iterables))
        )
        duration, _ = timestream(iterables_stream.flatten(concurrency=concurrency))
        self.assertAlmostEqual(
            duration,
            expected_iteration_duration,
            delta=expected_iteration_duration * DELTA_RATE,
            msg="Increasing the concurrency of mapping should decrease proportionnally the iteration's duration.",
        )

    @parameterized.expand(
        [
            [exception_type, mapped_exception_type, concurrency]
            for exception_type, mapped_exception_type in [
                (TestError, TestError),
                (StopIteration, NoopStopIteration),
            ]
            for concurrency in [1, 2]
        ]
    )
    def test_flatten_with_exception(
        self,
        exception_type: Type[Exception],
        mapped_exception_type: Type[Exception],
        concurrency: int,
    ) -> None:
        n_iterables = 5

        class IterableRaisingInIter(Iterable[int]):
            def __iter__(self) -> Iterator[int]:
                raise exception_type

        self.assertSetEqual(
            set(
                Stream(
                    map(
                        lambda i: (
                            IterableRaisingInIter() if i % 2 else range(i, i + 1)
                        ),
                        range(n_iterables),
                    )
                )
                .flatten(concurrency=concurrency)
                .catch(mapped_exception_type)
            ),
            set(range(0, n_iterables, 2)),
            msg="At any concurrency the `flatten` method should be resilient to exceptions thrown by iterators, especially it should remap StopIteration one to PacifiedStopIteration.",
        )

        class IteratorRaisingInNext(Iterator[int]):
            def __init__(self) -> None:
                self.first_next = True

            def __iter__(self) -> Iterator[int]:
                return self

            def __next__(self) -> int:
                if not self.first_next:
                    raise StopIteration
                self.first_next = False
                raise exception_type

        self.assertSetEqual(
            set(
                Stream(
                    map(
                        lambda i: (
                            IteratorRaisingInNext() if i % 2 else range(i, i + 1)
                        ),
                        range(n_iterables),
                    )
                )
                .flatten(concurrency=concurrency)
                .catch(mapped_exception_type)
            ),
            set(range(0, n_iterables, 2)),
            msg="At any concurrency the `flatten` method should be resilient to exceptions thrown by iterators, especially it should remap StopIteration one to PacifiedStopIteration.",
        )

    @parameterized.expand([[concurrency] for concurrency in [2, 4]])
    def test_partial_iteration_on_streams_using_concurrency(
        self, concurrency: int
    ) -> None:
        yielded_elems = []

        def remembering_src() -> Iterator[int]:
            nonlocal yielded_elems
            for elem in src:
                yielded_elems.append(elem)
                yield elem

        for stream in [
            Stream(remembering_src).map(identity, concurrency=concurrency),
            Stream(remembering_src).amap(async_identity, concurrency=concurrency),
            Stream(remembering_src).foreach(identity, concurrency=concurrency),
            Stream(remembering_src).aforeach(async_identity, concurrency=concurrency),
            Stream(remembering_src).group(1).flatten(concurrency=concurrency),
        ]:
            yielded_elems = []
            iterator = iter(stream)
            time.sleep(0.5)
            self.assertEqual(
                len(yielded_elems),
                0,
                msg=f"before the first call to `next` a concurrent {type(stream)} should have pulled 0 upstream elements.",
            )
            next(iterator)
            time.sleep(0.5)
            self.assertEqual(
                len(yielded_elems),
                concurrency + 1,
                msg=f"after the first call to `next` a concurrent {type(stream)} should have pulled only {concurrency + 1} (== concurrency + 1) upstream elements.",
            )

    def test_filter(self) -> None:
        def keep(x) -> Any:
            return x % 2

        self.assertListEqual(
            list(Stream(src).filter(keep)),
            list(filter(keep, src)),
            msg="`filter` must act like builtin filter",
        )
        self.assertListEqual(
            list(Stream(src).filter()),
            list(filter(None, src)),
            msg="`filter` without predicate must act like builtin filter with None predicate.",
        )

    def test_truncate(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "`count` and `when` can't be both None.",
        ):
            Stream(src).truncate()

        self.assertEqual(
            list(Stream(src).truncate(N * 2)),
            list(src),
            msg="`truncate` must be ok with count >= stream length",
        )
        self.assertEqual(
            list(Stream(src).truncate(2)),
            [0, 1],
            msg="`truncate` must be ok with count >= 1",
        )
        self.assertEqual(
            list(Stream(src).truncate(1)),
            [0],
            msg="`truncate` must be ok with count == 1",
        )
        self.assertEqual(
            list(Stream(src).truncate(0)),
            [],
            msg="`truncate` must be ok with count == 0",
        )

        with self.assertRaises(
            ValueError,
            msg="`truncate` must raise ValueError if `count` is negative",
        ):
            Stream(src).truncate(-1)

        with self.assertRaises(
            ValueError,
            msg="`truncate` must raise ValueError if `count` is float('inf')",
        ):
            Stream(src).truncate(cast(int, float("inf")))

        n_iterations = 0
        count = N // 2
        raising_stream_iterator = iter(
            Stream(lambda: map(lambda x: x / 0, src)).truncate(count)
        )
        while True:
            try:
                next(raising_stream_iterator)
            except ZeroDivisionError:
                n_iterations += 1
            except StopIteration:
                break
        self.assertEqual(
            n_iterations,
            count,
            msg="`truncate` must not stop iteration when encountering exceptions",
        )

        iter_truncated_on_predicate = iter(Stream(src).truncate(when=lambda n: n == 5))
        self.assertEqual(
            list(iter_truncated_on_predicate),
            list(Stream(src).truncate(5)),
            msg="`when` n == 5 must be equivalent to `count` = 5",
        )
        with self.assertRaises(
            StopIteration,
            msg="After exhaustion a call to __next__ on a truncated iterator must raise StopIteration",
        ):
            next(iter_truncated_on_predicate)

        with self.assertRaises(
            ZeroDivisionError,
            msg="an exception raised by `when` must be raised",
        ):
            list(Stream(src).truncate(when=lambda _: 1 / 0))

        self.assertEqual(
            list(Stream(src).truncate(6, when=lambda n: n == 5)),
            list(range(5)),
            msg="`when` and `count` argument can be set at the same time, and the truncation should happen as soon as one or the other is satisfied.",
        )

        self.assertEqual(
            list(Stream(src).truncate(5, when=lambda n: n == 6)),
            list(range(5)),
            msg="`when` and `count` argument can be set at the same time, and the truncation should happen as soon as one or the other is satisfied.",
        )

    def test_group(self) -> None:
        # behavior with invalid arguments
        for seconds in [-1, 0]:
            with self.assertRaises(
                ValueError,
                msg="`group` should raise error when called with `seconds` <= 0.",
            ):
                list(Stream([1]).group(size=100, seconds=seconds)),
        for size in [-1, 0]:
            with self.assertRaises(
                ValueError,
                msg="`group` should raise error when called with `size` < 1.",
            ):
                list(Stream([1]).group(size=size)),

        # group size
        self.assertListEqual(
            list(Stream(range(6)).group(size=4)),
            [[0, 1, 2, 3], [4, 5]],
            msg="",
        )
        self.assertListEqual(
            list(Stream(range(6)).group(size=2)),
            [[0, 1], [2, 3], [4, 5]],
            msg="",
        )
        self.assertListEqual(
            list(Stream([]).group(size=2)),
            [],
            msg="",
        )

        # behavior with exceptions
        def f(i):
            return i / (110 - i)

        stream_iterator = iter(Stream(lambda: map(f, src)).group(100))
        next(stream_iterator)
        self.assertListEqual(
            next(stream_iterator),
            list(map(f, range(100, 110))),
            msg="when encountering upstream exception, `group` should yield the current accumulated group...",
        )

        with self.assertRaises(
            ZeroDivisionError,
            msg="... and raise the upstream exception during the next call to `next`...",
        ):
            next(stream_iterator)

        self.assertListEqual(
            next(stream_iterator),
            list(map(f, range(111, 211))),
            msg="... and restarting a fresh group to yield after that.",
        )

        # behavior of the `seconds` parameter
        self.assertListEqual(
            list(
                Stream(lambda: map(slow_identity, src)).group(
                    size=100, seconds=0.9 * slow_identity_duration
                )
            ),
            list(map(lambda e: [e], src)),
            msg="`group` should yield each upstream element alone in a single-element group if `seconds` inferior to the upstream yield period",
        )
        self.assertListEqual(
            list(
                Stream(lambda: map(slow_identity, src)).group(
                    size=100, seconds=1.8 * slow_identity_duration
                )
            ),
            list(map(lambda e: [e, e + 1], pair_src)),
            msg="`group` should yield upstream elements in a two-element group if `seconds` inferior to twice the upstream yield period",
        )

        self.assertListEqual(
            next(iter(Stream(src).group())),
            list(src),
            msg="`group` without arguments should group the elements all together",
        )

        # test by
        stream_iter = iter(Stream(src).group(size=2, by=lambda n: n % 2))
        self.assertListEqual(
            [next(stream_iter), next(stream_iter)],
            [[0, 2], [1, 3]],
            msg="`group` called with a `by` function must cogroup elements.",
        )

        self.assertListEqual(
            next(
                iter(
                    Stream(src_raising_at_exhaustion).group(
                        size=10, by=lambda n: n % 4 != 0
                    )
                )
            ),
            [1, 2, 3, 5, 6, 7, 9, 10, 11, 13],
            msg="`group` called with a `by` function and a `size` should yield the first batch becoming full.",
        )

        self.assertListEqual(
            list(Stream(src).group(by=lambda n: n % 2)),
            [list(range(0, N, 2)), list(range(1, N, 2))],
            msg="`group` called with a `by` function and an infinite size must cogroup elements and yield groups starting with the group containing the oldest element.",
        )

        self.assertListEqual(
            list(Stream(range(10)).group(by=lambda n: n % 4 == 0)),
            [[0, 4, 8], [1, 2, 3, 5, 6, 7, 9]],
            msg="`group` called with a `by` function and reaching exhaustion must cogroup elements and yield uncomplete groups starting with the group containing the oldest element, even though it's not the largest.",
        )

        stream_iter = iter(Stream(src_raising_at_exhaustion).group(by=lambda n: n % 2))
        self.assertListEqual(
            [next(stream_iter), next(stream_iter)],
            [list(range(0, N, 2)), list(range(1, N, 2))],
            msg="`group` called with a `by` function and encountering an exception must cogroup elements and yield uncomplete groups starting with the group containing the oldest element.",
        )
        with self.assertRaises(
            TestError,
            msg="`group` called with a `by` function and encountering an exception must raise it after all groups have been yielded",
        ):
            next(stream_iter)

        # test seconds + by
        self.assertListEqual(
            list(
                Stream(lambda: map(slow_identity, range(10))).group(
                    seconds=slow_identity_duration * 2.90, by=lambda n: n % 4 == 0
                )
            ),
            [[1, 2], [0, 4], [3, 5, 6, 7], [8], [9]],
            msg="`group` called with a `by` function must cogroup elements and yield the largest groups when `seconds` is reached event though it's not the oldest.",
        )

        stream_iter = iter(
            Stream(src).group(
                size=3, by=lambda n: throw(StopIteration) if n == 2 else n
            )
        )
        self.assertEqual(
            [next(stream_iter), next(stream_iter)],
            [[0], [1]],
            msg="`group` should yield incomplete groups when `by` raises",
        )
        with self.assertRaises(
            NoopStopIteration,
            msg="`group` should raise and skip `elem` if `by(elem)` raises",
        ):
            next(stream_iter)
        self.assertEqual(
            next(stream_iter),
            [3],
            msg="`group` should continue yielding after `by`'s exception has been raised.",
        )

    def test_slow(self) -> None:
        # behavior with invalid arguments
        for frequency in [-0.01, 0]:
            with self.assertRaises(
                ValueError,
                msg="`slow` should raise error when called with `frequency` <= 0.",
            ):
                list(Stream([1]).slow(frequency=frequency))

        frequency = 3
        period = 1 / frequency
        super_slow_elem_pull_seconds = 1
        N = 10
        expected_duration = (N - 1) * period + super_slow_elem_pull_seconds
        duration, _ = timestream(
            Stream(range(N))
            .foreach(
                lambda e: time.sleep(super_slow_elem_pull_seconds) if e == 0 else None
            )
            .slow(frequency=frequency)
        )
        self.assertAlmostEqual(
            duration,
            expected_duration,
            delta=0.1 * expected_duration,
            msg="avoid bursts after very slow particular upstream elements",
        )

        # float or int frequency
        for frequency in [0.9, 2, 50]:
            self.assertEqual(
                next(
                    iter(
                        Stream(src)
                        .slow(frequency=frequency)
                        .slow(frequency=frequency * 2)
                    )
                ),
                0,
                msg="`slow` should avoid 'ValueError: sleep length must be non-negative'",
            )

            stream_iterator = iter(Stream(src).slow(frequency=frequency))
            start_time = time.time()
            a = next(stream_iterator)
            b = next(stream_iterator)
            c = next(stream_iterator)
            end_time = time.time()

            self.assertEqual(
                a, 0, msg="`slow` must forward upstream elements unchanged"
            )
            self.assertEqual(
                b, 1, msg="`slow` must forward upstream elements unchanged"
            )
            self.assertEqual(
                c, 2, msg="`slow` must forward upstream elements unchanged"
            )

            period = 1 / frequency
            expected_duration = 3 * period
            self.assertAlmostEqual(
                end_time - start_time,
                expected_duration,
                delta=0.3 * expected_duration,
                msg="`slow` must respect the frequency set.",
            )

    def test_catch(self) -> None:
        def f(i):
            return i / (3 - i)

        stream = Stream(lambda: map(f, src))
        safe_src = list(src)
        del safe_src[3]
        self.assertListEqual(
            list(stream.catch(ZeroDivisionError)),
            list(map(f, safe_src)),
            msg="If the exception type matches the `kind`, then the impacted element should be ignored.",
        )
        self.assertListEqual(
            list(stream.catch()),
            list(map(f, safe_src)),
            msg="If the predicate is not provided, then all exceptions should be catched.",
        )

        with self.assertRaises(
            ZeroDivisionError,
            msg="If a non catched exception type occurs, then it should be raised.",
        ):
            list(stream.catch(TestError))

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

        erroring_stream: Stream[int] = Stream(lambda: map(lambda f: f(), functions))
        for catched_erroring_stream in [
            erroring_stream.catch(finally_raise=True),
            erroring_stream.catch(Exception, finally_raise=True),
        ]:
            erroring_stream_iterator = iter(catched_erroring_stream)
            self.assertEqual(
                next(erroring_stream_iterator),
                first_value,
                msg="`catch` should yield the first non exception throwing element.",
            )
            n_yields = 1
            with self.assertRaises(
                TestError,
                msg="`catch` should raise the first error encountered when `finally_raise` is True.",
            ):
                for _ in erroring_stream_iterator:
                    n_yields += 1
            with self.assertRaises(
                StopIteration,
                msg="`catch` with `finally_raise`=True should finally raise StopIteration to avoid infinite recursion if there is another catch downstream.",
            ):
                next(erroring_stream_iterator)
            self.assertEqual(
                n_yields,
                3,
                msg="3 elements should have passed been yielded between catched exceptions.",
            )

        only_catched_errors_stream = Stream(
            map(lambda _: throw(TestError), range(2000))
        ).catch(TestError)
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

        iterator = iter(
            Stream(map(throw, [TestError, ValueError]))
            .catch(ValueError, finally_raise=True)
            .catch(TestError, finally_raise=True)
        )
        with self.assertRaises(
            ValueError,
            msg="With 2 chained `catch`s with `finally_raise=True`, the error catched by the first `catch` is finally raised first (even though it was raised second)...",
        ):
            next(iterator)
        with self.assertRaises(
            TestError,
            msg="... and then the error catched by the second `catch` is raised...",
        ):
            next(iterator)
        with self.assertRaises(
            StopIteration,
            msg="... and a StopIteration is raised next.",
        ):
            next(iterator)

    def test_observe(self) -> None:
        value_error_rainsing_stream: Stream[List[int]] = (
            Stream("123--567")
            .slow(1)
            .observe("chars")
            .map(int)
            .observe("ints")
            .group(2)
            .observe("int pairs")
        )

        self.assertListEqual(
            list(value_error_rainsing_stream.catch(ValueError)),
            [[1, 2], [3], [5, 6], [7]],
            msg="This can break due to `group`/`map`/`catch`, check other breaking tests to determine quickly if it's an issue with `observe`.",
        )

        with self.assertRaises(
            ValueError,
            msg="`observe` should forward-raise exceptions",
        ):
            list(value_error_rainsing_stream)

    def test_is_iterable(self) -> None:
        self.assertIsInstance(Stream(src), Iterable)

    def test_exhaust(self) -> None:
        l: List[int] = []

        def effect(x: int) -> None:
            nonlocal l
            l.append(x)

        self.assertEqual(
            Stream(lambda: map(effect, src)).exhaust(),
            N,
            msg="`__len__` should return the number of iterated elements.",
        )
        self.assertListEqual(
            l, list(src), msg="`__len__` should iterate over the entire stream."
        )

    def test_multiple_iterations(self) -> None:
        stream = Stream(src)
        for _ in range(3):
            self.assertEqual(
                list(stream),
                list(src),
                msg="The first iteration over a stream should yield the same elements as any subsequent iteration on the same stream, even if it is based on a `source` returning an iterator that only support 1 iteration.",
            )

    @parameterized.expand(
        [
            [1],
            [100],
        ]
    )
    def test_amap(self, concurrency) -> None:
        self.assertListEqual(
            list(
                Stream(less_and_less_slow_src).amap(
                    async_square, concurrency=concurrency
                )
            ),
            list(map(square, src)),
            msg="At any concurrency the `amap` method should act as the builtin map function, transforming elements while preserving input elements order.",
        )
        stream = Stream(src).amap(identity)  # type: ignore
        with self.assertRaisesRegex(
            TypeError,
            "The function is expected to be an async function, i.e. it must be a function returning a Coroutine object, but returned a <class 'int'>.",
            msg="`amap` should raise a TypeError if a non async function is passed to it.",
        ):
            next(iter(stream))

    @parameterized.expand(
        [
            [1],
            [100],
        ]
    )
    def test_aforeach(self, concurrency) -> None:
        self.assertListEqual(
            list(
                Stream(less_and_less_slow_src).aforeach(
                    async_square, concurrency=concurrency
                )
            ),
            list(src),
            msg="At any concurrency the `foreach` method must preserve input elements order.",
        )
        stream = Stream(src).aforeach(identity)  # type: ignore
        with self.assertRaisesRegex(
            TypeError,
            "The function is expected to be an async function, i.e. it must be a function returning a Coroutine object, but returned a <class 'int'>.",
            msg="`aforeach` should raise a TypeError if a non async function is passed to it.",
        ):
            next(iter(stream))
