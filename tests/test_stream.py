import asyncio
import copy
import datetime
import logging
import math
import queue
import sys
import threading
import time
import traceback
import unittest
from collections import Counter
from functools import partial
from pickle import PickleError
from types import TracebackType
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

from parameterized import parameterized  # type: ignore

from streamable import Stream
from streamable.util.asynctools import awaitable_to_coroutine
from streamable.util.functiontools import WrappedError, asyncify, star
from streamable.util.iterabletools import (
    sync_to_async_iter,
    sync_to_bi_iterable,
)
from tests.utils import (
    DELTA_RATE,
    ITERABLE_TYPES,
    IterableType,
    N,
    TestError,
    alist_or_list,
    anext_or_next,
    async_identity,
    async_identity_sleep,
    async_randomly_slowed,
    async_slow_identity,
    async_square,
    async_throw_for_odd_func,
    async_throw_func,
    bi_iterable_to_iter,
    even_src,
    identity,
    identity_sleep,
    stopiteration_for_iter_type,
    randomly_slowed,
    slow_identity,
    slow_identity_duration,
    square,
    src,
    src_raising_at_exhaustion,
    throw,
    throw_for_odd_func,
    throw_func,
    timestream,
    to_list,
    to_set,
)


class TestStream(unittest.TestCase):
    def test_init(self) -> None:
        stream = Stream(src)
        self.assertIs(
            stream._source,
            src,
            msg="The stream's `source` must be the source argument.",
        )
        self.assertIsNone(
            stream.upstream,
            msg="The `upstream` attribute of a base Stream's instance must be None.",
        )

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
            .throttle(1, per=datetime.timedelta(seconds=1))
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

    @parameterized.expand(ITERABLE_TYPES)
    def test_async_src(self, itype) -> None:
        self.assertEqual(
            to_list(Stream(sync_to_async_iter(src)), itype),
            list(src),
            msg="a stream with an async source must be collectable as an Iterable or as AsyncIterable",
        )
        self.assertEqual(
            to_list(Stream(sync_to_async_iter(src).__aiter__), itype),
            list(src),
            msg="a stream with an async source must be collectable as an Iterable or as AsyncIterable",
        )

    def test_repr_and_display(self) -> None:
        class CustomCallable:
            pass

        complex_stream: Stream[int] = (
            Stream(src)
            .truncate(1024, when=lambda _: False)
            .atruncate(1024, when=async_identity)
            .skip(10)
            .askip(10)
            .skip(until=lambda _: True)
            .askip(until=async_identity)
            .distinct(lambda _: _)
            .adistinct(async_identity)
            .filter()
            .map(lambda i: (i,))
            .map(lambda i: (i,), concurrency=2)
            .filter(star(bool))
            .afilter(star(async_identity))
            .foreach(lambda _: _)
            .foreach(lambda _: _, concurrency=2)
            .aforeach(async_identity)
            .map(cast(Callable[[Any], Any], CustomCallable()))
            .amap(async_identity)
            .group(100)
            .agroup(100)
            .groupby(len)
            .agroupby(async_identity)
            .map(star(lambda key, group: group))
            .observe("groups")
            .flatten(concurrency=4)
            .map(sync_to_async_iter)
            .aflatten(concurrency=4)
            .map(lambda _: 0)
            .throttle(
                64,
                per=datetime.timedelta(seconds=1),
            )
            .observe("foos")
            .catch(finally_raise=True, when=identity)
            .acatch(finally_raise=True, when=async_identity)
            .catch((TypeError, ValueError, None, ZeroDivisionError))
            .acatch((TypeError, ValueError, None, ZeroDivisionError))
            .catch(TypeError, replacement=1, finally_raise=True)
            .acatch(TypeError, replacement=1, finally_raise=True)
        )

        print(repr(complex_stream))

        explanation_1 = str(complex_stream)

        explanation_2 = str(complex_stream.map(str))
        self.assertNotEqual(
            explanation_1,
            explanation_2,
            msg="explanation of different streams must be different",
        )

        print(explanation_1)

        complex_stream.display()
        complex_stream.display(logging.ERROR)

        self.assertEqual(
            str(Stream(src)),
            "Stream(range(0, 256))",
            msg="`repr` should work as expected on a stream without operation",
        )
        self.assertEqual(
            str(Stream(src).skip(10)),
            "Stream(range(0, 256)).skip(10, until=None)",
            msg="`repr` should return a one-liner for a stream with 1 operations",
        )
        self.assertEqual(
            str(Stream(src).skip(10).skip(10)),
            "Stream(range(0, 256)).skip(10, until=None).skip(10, until=None)",
            msg="`repr` should return a one-liner for a stream with 2 operations",
        )
        self.assertEqual(
            str(Stream(src).skip(10).skip(10).skip(10)),
            """(
    Stream(range(0, 256))
    .skip(10, until=None)
    .skip(10, until=None)
    .skip(10, until=None)
)""",
            msg="`repr` should go to line for a stream with 3 operations",
        )
        self.assertEqual(
            str(complex_stream),
            """(
    Stream(range(0, 256))
    .truncate(count=1024, when=<lambda>)
    .atruncate(count=1024, when=async_identity)
    .skip(10, until=None)
    .askip(10, until=None)
    .skip(None, until=<lambda>)
    .askip(None, until=async_identity)
    .distinct(<lambda>, consecutive_only=False)
    .adistinct(async_identity, consecutive_only=False)
    .filter(bool)
    .map(<lambda>, concurrency=1, ordered=True)
    .map(<lambda>, concurrency=2, ordered=True, via='thread')
    .filter(star(bool))
    .afilter(star(async_identity))
    .foreach(<lambda>, concurrency=1, ordered=True)
    .foreach(<lambda>, concurrency=2, ordered=True, via='thread')
    .aforeach(async_identity, concurrency=1, ordered=True)
    .map(CustomCallable(...), concurrency=1, ordered=True)
    .amap(async_identity, concurrency=1, ordered=True)
    .group(size=100, by=None, interval=None)
    .agroup(size=100, by=None, interval=None)
    .groupby(len, size=None, interval=None)
    .agroupby(async_identity, size=None, interval=None)
    .map(star(<lambda>), concurrency=1, ordered=True)
    .observe('groups')
    .flatten(concurrency=4)
    .map(SyncToAsyncIterator, concurrency=1, ordered=True)
    .aflatten(concurrency=4)
    .map(<lambda>, concurrency=1, ordered=True)
    .throttle(64, per=datetime.timedelta(seconds=1))
    .observe('foos')
    .catch(Exception, when=identity, finally_raise=True)
    .acatch(Exception, when=async_identity, finally_raise=True)
    .catch((TypeError, ValueError, None, ZeroDivisionError), when=None, finally_raise=False)
    .acatch((TypeError, ValueError, None, ZeroDivisionError), when=None, finally_raise=False)
    .catch(TypeError, when=None, replacement=1, finally_raise=True)
    .acatch(TypeError, when=None, replacement=1, finally_raise=True)
)""",
            msg="`repr` should work as expected on a stream with many operation",
        )

    @parameterized.expand(ITERABLE_TYPES)
    def test_iter(self, itype: IterableType) -> None:
        self.assertIsInstance(
            bi_iterable_to_iter(Stream(src), itype=itype),
            itype,
            msg="iter(stream) must return an Iterator.",
        )

        with self.assertRaisesRegex(
            TypeError,
            r"`source` must be an Iterable/AsyncIterable or a Callable\[\[\], Iterable/AsyncIterable\] but got a <class 'int'>",
            msg="Getting an Iterator from a Stream with a source not being a Union[Callable[[], Iterator], ITerable] must raise TypeError.",
        ):
            bi_iterable_to_iter(Stream(1), itype=itype)  # type: ignore

        with self.assertRaisesRegex(
            TypeError,
            r"`source` must be an Iterable/AsyncIterable or a Callable\[\[\], Iterable/AsyncIterable\] but got a Callable\[\[\], <class 'int'>\]",
            msg="Getting an Iterator from a Stream with a source not being a Union[Callable[[], Iterator], ITerable] must raise TypeError.",
        ):
            bi_iterable_to_iter(Stream(lambda: 1), itype=itype)  # type: ignore

    @parameterized.expand(ITERABLE_TYPES)
    def test_add(self, itype: IterableType) -> None:
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
            to_list(stream_a + stream_b + stream_c, itype=itype),
            list(range(30)),
            msg="`chain` must yield the elements of the first stream the move on with the elements of the next ones and so on.",
        )

    @parameterized.expand(
        [
            [Stream.map, [identity]],
            [Stream.amap, [async_identity]],
            [Stream.foreach, [identity]],
            [Stream.aforeach, [identity]],
            [Stream.flatten, []],
            [Stream.aflatten, []],
        ]
    )
    def test_sanitize_concurrency(self, method, args) -> None:
        stream = Stream(src)
        with self.assertRaises(
            TypeError,
            msg=f"`{method}` should be raising TypeError for non-int concurrency.",
        ):
            method(stream, *args, concurrency="1")

        with self.assertRaises(
            ValueError,
            msg=f"`{method}` should be raising ValueError for concurrency=0.",
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
            (Stream.map,),
            (Stream.foreach,),
        ]
    )
    def test_sanitize_via(self, method) -> None:
        with self.assertRaisesRegex(
            TypeError,
            "`via` must be 'thread' or 'process' but got 'foo'",
            msg=f"`{method}` must raise a TypeError for invalid via",
        ):
            method(Stream(src), identity, via="foo")

    @parameterized.expand(
        [(concurrency, itype) for concurrency in (1, 2) for itype in ITERABLE_TYPES]
    )
    def test_map(self, concurrency, itype) -> None:
        self.assertListEqual(
            to_list(
                Stream(src).map(randomly_slowed(square), concurrency=concurrency),
                itype=itype,
            ),
            list(map(square, src)),
            msg="At any concurrency the `map` method should act as the builtin map function, transforming elements while preserving input elements order.",
        )

    @parameterized.expand(
        [
            (ordered, order_mutation, itype)
            for itype in ITERABLE_TYPES
            for ordered, order_mutation in [
                (True, identity),
                (False, sorted),
            ]
        ]
    )
    def test_process_concurrency(
        self, ordered, order_mutation, itype
    ) -> None:  # pragma: no cover
        # 3.7 and 3.8 are passing the test but hang forever after
        if sys.version_info.minor < 9:
            return

        lambda_identity = lambda x: x * 10

        def local_identity(x):
            return x

        for f in [lambda_identity, local_identity]:
            with self.assertRaisesRegex(
                (AttributeError, PickleError),
                "<locals>",
                msg="process-based concurrency should not be able to serialize a lambda or a local func",
            ):
                to_list(Stream(src).map(f, concurrency=2, via="process"), itype=itype)

        sleeps = [0.01, 1, 0.01]
        state: List[str] = []
        expected_result_list: List[str] = list(order_mutation(map(str, sleeps)))
        stream = (
            Stream(sleeps)
            .foreach(identity_sleep, concurrency=2, ordered=ordered, via="process")
            .map(str, concurrency=2, ordered=True, via="process")
            .foreach(state.append, concurrency=2, ordered=True, via="process")
            .foreach(lambda _: state.append(""), concurrency=1, ordered=True)
        )
        self.assertListEqual(
            to_list(stream, itype=itype),
            expected_result_list,
            msg="process-based concurrency must correctly transform elements, respecting `ordered`...",
        )
        self.assertListEqual(
            state,
            [""] * len(sleeps),
            msg="... and must not mutate main thread-bound structures.",
        )
        # test partial iteration:
        self.assertEqual(
            anext_or_next(bi_iterable_to_iter(stream, itype=itype)),
            expected_result_list[0],
            msg="process-based concurrency must behave ok with partial iteration",
        )

    @parameterized.expand(
        [
            (concurrency, n_elems, itype)
            for concurrency, n_elems in [
                [16, 0],
                [1, 0],
                [16, 1],
                [16, 15],
                [16, 16],
            ]
            for itype in ITERABLE_TYPES
        ]
    )
    def test_map_with_more_concurrency_than_elements(
        self, concurrency, n_elems, itype
    ) -> None:
        self.assertListEqual(
            to_list(
                Stream(range(n_elems)).map(str, concurrency=concurrency), itype=itype
            ),
            list(map(str, range(n_elems))),
            msg="`map` method should act correctly when concurrency > number of elements.",
        )

    @parameterized.expand(
        [
            [ordered, order_mutation, expected_duration, operation, func, itype]
            for ordered, order_mutation, expected_duration in [
                (True, identity, 0.3),
                (False, sorted, 0.21),
            ]
            for operation, func in [
                (Stream.foreach, time.sleep),
                (Stream.map, identity_sleep),
                (Stream.aforeach, asyncio.sleep),
                (Stream.amap, async_identity_sleep),
            ]
            for itype in ITERABLE_TYPES
        ]
    )
    def test_mapping_ordering(
        self,
        ordered: bool,
        order_mutation: Callable[[Iterable[float]], Iterable[float]],
        expected_duration: float,
        operation,
        func,
        itype,
    ) -> None:
        seconds = [0.1, 0.01, 0.2]
        duration, res = timestream(
            operation(Stream(seconds), func, ordered=ordered, concurrency=2),
            5,
            itype=itype,
        )
        self.assertListEqual(
            res,
            list(order_mutation(seconds)),
            msg=f"`{operation}` must respect `ordered` constraint.",
        )

        self.assertAlmostEqual(
            duration,
            expected_duration,
            msg=f"{'ordered' if ordered else 'unordered'} `{operation}` should reflect that unordering improves runtime by avoiding bottlenecks",
            delta=expected_duration * 0.2,
        )

    @parameterized.expand(
        [(concurrency, itype) for concurrency in (1, 2) for itype in ITERABLE_TYPES]
    )
    def test_foreach(self, concurrency, itype) -> None:
        side_collection: Set[int] = set()

        def side_effect(x: int, func: Callable[[int], int]):
            nonlocal side_collection
            side_collection.add(func(x))

        res = to_list(
            Stream(src).foreach(
                lambda i: randomly_slowed(side_effect(i, square)),
                concurrency=concurrency,
            ),
            itype=itype,
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
                caught_exc,
                concurrency,
                method,
                throw_func_,
                throw_for_odd_func_,
                itype,
            ]
            for raised_exc, caught_exc in [
                (TestError, (TestError,)),
                (StopIteration, (WrappedError, RuntimeError)),
            ]
            for concurrency in [1, 2]
            for method, throw_func_, throw_for_odd_func_ in [
                (Stream.foreach, throw_func, throw_for_odd_func),
                (Stream.aforeach, async_throw_func, async_throw_for_odd_func),
                (Stream.map, throw_func, throw_for_odd_func),
                (Stream.amap, async_throw_func, async_throw_for_odd_func),
            ]
            for itype in ITERABLE_TYPES
        ]
    )
    def test_map_or_foreach_with_exception(
        self,
        raised_exc: Type[Exception],
        caught_exc: Tuple[Type[Exception], ...],
        concurrency: int,
        method: Callable[[Stream, Callable[[Any], int], int], Stream],
        throw_func: Callable[[Exception], Callable[[Any], int]],
        throw_for_odd_func: Callable[[Type[Exception]], Callable[[Any], int]],
        itype: IterableType,
    ) -> None:
        with self.assertRaises(
            caught_exc,
            msg="At any concurrency, `map` and `foreach` and `amap` must raise",
        ):
            to_list(
                method(Stream(src), throw_func(raised_exc), concurrency=concurrency),  # type: ignore
                itype=itype,
            )

        self.assertListEqual(
            to_list(
                method(
                    Stream(src),
                    throw_for_odd_func(raised_exc),
                    concurrency=concurrency,  # type: ignore
                ).catch(caught_exc),
                itype=itype,
            ),
            list(even_src),
            msg="At any concurrency, `map` and `foreach` and `amap` must not stop after one exception occured.",
        )

    @parameterized.expand(
        [
            [method, func, concurrency, itype]
            for method, func in [
                (Stream.foreach, slow_identity),
                (Stream.aforeach, async_slow_identity),
                (Stream.map, slow_identity),
                (Stream.amap, async_slow_identity),
            ]
            for concurrency in [1, 2, 4]
            for itype in ITERABLE_TYPES
        ]
    )
    def test_map_and_foreach_concurrency(
        self, method, func, concurrency, itype
    ) -> None:
        expected_iteration_duration = N * slow_identity_duration / concurrency
        duration, res = timestream(
            method(Stream(src), func, concurrency=concurrency), itype=itype
        )
        self.assertListEqual(res, list(src))
        self.assertAlmostEqual(
            duration,
            expected_iteration_duration,
            delta=expected_iteration_duration * DELTA_RATE,
            msg="Increasing the concurrency of mapping should decrease proportionnally the iteration's duration.",
        )

    @parameterized.expand(
        [
            (concurrency, itype, flatten)
            for concurrency in (1, 2)
            for itype in ITERABLE_TYPES
            for flatten in (Stream.flatten, Stream.aflatten)
        ]
    )
    def test_flatten(self, concurrency, itype, flatten) -> None:
        n_iterables = 32
        it = list(range(N // n_iterables))
        double_it = it + it
        iterables_stream = Stream(
            [sync_to_bi_iterable(double_it)]
            + [sync_to_bi_iterable(it) for _ in range(n_iterables)]
        ).map(slow_identity)
        self.assertCountEqual(
            to_list(flatten(iterables_stream, concurrency=concurrency), itype=itype),
            list(it) * n_iterables + double_it,
            msg="At any concurrency the `flatten` method should yield all the upstream iterables' elements.",
        )
        self.assertListEqual(
            to_list(
                flatten(
                    Stream([sync_to_bi_iterable(iter([])) for _ in range(2000)]),
                    concurrency=concurrency,
                ),
                itype=itype,
            ),
            [],
            msg="`flatten` should not yield any element if upstream elements are empty iterables, and be resilient to recursion issue in case of successive empty upstream iterables.",
        )

        with self.assertRaises(
            (TypeError, AttributeError),
            msg="`flatten` should raise if an upstream element is not iterable.",
        ):
            anext_or_next(
                bi_iterable_to_iter(
                    flatten(Stream(cast(Union[Iterable, AsyncIterable], src))),
                    itype=itype,
                )
            )

        # test typing with ranges
        _: Stream[int] = Stream((src, src)).flatten()

    @parameterized.expand(
        [
            (flatten, itype, slow)
            for flatten, slow in (
                (Stream.flatten, partial(Stream.map, transformation=slow_identity)),
                (
                    Stream.aflatten,
                    partial(Stream.amap, transformation=async_slow_identity),
                ),
            )
            for itype in ITERABLE_TYPES
        ]
    )
    def test_flatten_concurrency(self, flatten, itype, slow) -> None:
        concurrency = 2
        iterable_size = 5
        runtime, res = timestream(
            flatten(
                Stream(
                    lambda: [
                        slow(Stream(["a"] * iterable_size)),
                        slow(Stream(["b"] * iterable_size)),
                        slow(Stream(["c"] * iterable_size)),
                    ]
                ),
                concurrency=concurrency,
            ),
            times=3,
            itype=itype,
        )
        self.assertListEqual(
            res,
            ["a", "b"] * iterable_size + ["c"] * iterable_size,
            msg="`flatten` should process 'a's and 'b's concurrently and then 'c's",
        )
        a_runtime = b_runtime = c_runtime = iterable_size * slow_identity_duration
        expected_runtime = (a_runtime + b_runtime) / concurrency + c_runtime
        self.assertAlmostEqual(
            runtime,
            expected_runtime,
            delta=DELTA_RATE * expected_runtime,
            msg="`flatten` should process 'a's and 'b's concurrently and then 'c's without concurrency",
        )

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

        flattened_asynciter_stream: Stream[str] = (
            Stream("abc").map(sync_to_async_iter).aflatten()
        )

    @parameterized.expand(
        [
            [exception_type, mapped_exception_type, concurrency, itype, flatten]
            for concurrency in [1, 2]
            for itype in ITERABLE_TYPES
            for exception_type, mapped_exception_type in [
                (TestError, TestError),
                (stopiteration_for_iter_type(itype), (WrappedError, RuntimeError)),
            ]
            for flatten in (Stream.flatten, Stream.aflatten)
        ]
    )
    def test_flatten_with_exception_in_iter(
        self,
        exception_type: Type[Exception],
        mapped_exception_type: Type[Exception],
        concurrency: int,
        itype: IterableType,
        flatten: Callable,
    ) -> None:
        n_iterables = 5

        class IterableRaisingInIter(Iterable[int]):
            def __iter__(self) -> Iterator[int]:
                raise exception_type

        res: Set[int] = to_set(
            flatten(
                Stream(
                    map(
                        lambda i: sync_to_bi_iterable(
                            IterableRaisingInIter() if i % 2 else range(i, i + 1)
                        ),
                        range(n_iterables),
                    )
                ),
                concurrency=concurrency,
            ).catch(mapped_exception_type),
            itype=itype,
        )
        self.assertSetEqual(
            res,
            set(range(0, n_iterables, 2)),
            msg="At any concurrency the `flatten` method should be resilient to exceptions thrown by iterators, especially it should wrap Stop(Async)Iteration.",
        )

    @parameterized.expand(
        [
            [concurrency, itype, flatten]
            for concurrency in [1, 2]
            for itype in ITERABLE_TYPES
            for flatten in (Stream.flatten, Stream.aflatten)
        ]
    )
    def test_flatten_with_exception_in_next(
        self,
        concurrency: int,
        itype: IterableType,
        flatten: Callable,
    ) -> None:
        n_iterables = 5

        class IteratorRaisingInNext(Iterator[int]):
            def __init__(self) -> None:
                self.first_next = True

            def __next__(self) -> int:
                if not self.first_next:
                    raise StopIteration()
                self.first_next = False
                raise TestError

        res = to_set(
            flatten(
                Stream(
                    map(
                        lambda i: (
                            sync_to_bi_iterable(
                                IteratorRaisingInNext() if i % 2 else range(i, i + 1)
                            )
                        ),
                        range(n_iterables),
                    )
                ),
                concurrency=concurrency,
            ).catch(TestError),
            itype=itype,
        )
        self.assertSetEqual(
            res,
            set(range(0, n_iterables, 2)),
            msg="At any concurrency the `flatten` method should be resilient to exceptions thrown by iterators, especially it should wrap Stop(Async)Iteration.",
        )

    @parameterized.expand(
        [(concurrency, itype) for concurrency in [2, 4] for itype in ITERABLE_TYPES]
    )
    def test_partial_iteration_on_streams_using_concurrency(
        self, concurrency: int, itype: IterableType
    ) -> None:
        yielded_elems = []

        def remembering_src() -> Iterator[int]:
            nonlocal yielded_elems
            for elem in src:
                yielded_elems.append(elem)
                yield elem

        for stream, n_pulls_after_first_next in [
            (
                Stream(remembering_src).map(identity, concurrency=concurrency),
                concurrency + 1,
            ),
            (
                Stream(remembering_src).amap(async_identity, concurrency=concurrency),
                concurrency + 1,
            ),
            (
                Stream(remembering_src).foreach(identity, concurrency=concurrency),
                concurrency + 1,
            ),
            (
                Stream(remembering_src).aforeach(
                    async_identity, concurrency=concurrency
                ),
                concurrency + 1,
            ),
            (
                Stream(remembering_src).group(1).flatten(concurrency=concurrency),
                concurrency,
            ),
        ]:
            yielded_elems = []
            iterator = bi_iterable_to_iter(stream, itype=itype)
            time.sleep(0.5)
            self.assertEqual(
                len(yielded_elems),
                0,
                msg=f"before the first call to `next` a concurrent {type(stream)} should have pulled 0 upstream elements.",
            )
            anext_or_next(iterator)
            time.sleep(0.5)
            self.assertEqual(
                len(yielded_elems),
                n_pulls_after_first_next,
                msg=f"`after the first call to `next` a concurrent {type(stream)} with concurrency={concurrency} should have pulled only {n_pulls_after_first_next} upstream elements.",
            )

    @parameterized.expand(ITERABLE_TYPES)
    def test_filter(self, itype: IterableType) -> None:
        def keep(x) -> int:
            return x % 2

        self.assertListEqual(
            to_list(Stream(src).filter(keep), itype=itype),
            list(filter(keep, src)),
            msg="`filter` must act like builtin filter",
        )
        self.assertListEqual(
            to_list(Stream(src).filter(bool), itype=itype),
            list(filter(None, src)),
            msg="`filter` with `bool` as predicate must act like builtin filter with None predicate.",
        )
        self.assertListEqual(
            to_list(Stream(src).filter(), itype=itype),
            list(filter(None, src)),
            msg="`filter` without predicate must act like builtin filter with None predicate.",
        )
        self.assertListEqual(
            to_list(Stream(src).filter(None), itype=itype),  # type: ignore
            list(filter(None, src)),
            msg="`filter` with None predicate must act unofficially like builtin filter with None predicate.",
        )

        self.assertEqual(
            to_list(Stream(src).filter(None), itype=itype),  # type: ignore
            list(filter(None, src)),
            msg="Unofficially accept `stream.filter(None)`, behaving as builtin `filter(None, iter)`",
        )
        # with self.assertRaisesRegex(
        #     TypeError,
        #     "`when` cannot be None",
        #     msg="`filter` does not accept a None predicate",
        # ):
        #     to_list(Stream(src).filter(None), itype=itype)  # type: ignore

    @parameterized.expand(ITERABLE_TYPES)
    def test_afilter(self, itype: IterableType) -> None:
        def keep(x) -> int:
            return x % 2

        async def async_keep(x) -> int:
            return keep(x)

        self.assertListEqual(
            to_list(Stream(src).afilter(async_keep), itype=itype),
            list(filter(keep, src)),
            msg="`afilter` must act like builtin filter",
        )
        self.assertListEqual(
            to_list(Stream(src).afilter(asyncify(bool)), itype=itype),
            list(filter(None, src)),
            msg="`afilter` with `bool` as predicate must act like builtin filter with None predicate.",
        )
        self.assertListEqual(
            to_list(Stream(src).afilter(None), itype=itype),  # type: ignore
            list(filter(None, src)),
            msg="`afilter` with None predicate must act unofficially like builtin filter with None predicate.",
        )

        self.assertEqual(
            to_list(Stream(src).afilter(None), itype=itype),  # type: ignore
            list(filter(None, src)),
            msg="Unofficially accept `stream.afilter(None)`, behaving as builtin `filter(None, iter)`",
        )
        # with self.assertRaisesRegex(
        #     TypeError,
        #     "`when` cannot be None",
        #     msg="`afilter` does not accept a None predicate",
        # ):
        #     to_list(Stream(src).afilter(None), itype=itype)  # type: ignore

    @parameterized.expand(ITERABLE_TYPES)
    def test_skip(self, itype: IterableType) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "`count` must be >= 0 but got -1",
            msg="`skip` must raise ValueError if `count` is negative",
        ):
            Stream(src).skip(-1)

        self.assertListEqual(
            to_list(Stream(src).skip(), itype=itype),
            list(src),
            msg="`skip` must be no-op if both `count` and `until` are None",
        )

        self.assertListEqual(
            to_list(Stream(src).skip(None), itype=itype),
            list(src),
            msg="`skip` must be no-op if both `count` and `until` are None",
        )

        for count in [0, 1, 3]:
            self.assertListEqual(
                to_list(Stream(src).skip(count), itype=itype),
                list(src)[count:],
                msg="`skip` must skip `count` elements",
            )

            self.assertListEqual(
                to_list(
                    Stream(map(throw_for_odd_func(TestError), src))
                    .skip(count)
                    .catch(TestError),
                    itype=itype,
                ),
                list(filter(lambda i: i % 2 == 0, src))[count:],
                msg="`skip` must not count exceptions as skipped elements",
            )

            self.assertListEqual(
                to_list(Stream(src).skip(until=lambda n: n >= count), itype=itype),
                list(src)[count:],
                msg="`skip` must yield starting from the first element satisfying `until`",
            )

            self.assertListEqual(
                to_list(Stream(src).skip(count, until=lambda n: False), itype=itype),
                list(src)[count:],
                msg="`skip` must ignore `count` elements if `until` is never satisfied",
            )

            self.assertListEqual(
                to_list(
                    Stream(src).skip(count * 2, until=lambda n: n >= count), itype=itype
                ),
                list(src)[count:],
                msg="`skip` must ignore less than `count` elements if `until` is satisfied first",
            )

        self.assertListEqual(
            to_list(Stream(src).skip(until=lambda n: False), itype=itype),
            [],
            msg="`skip` must not yield any element if `until` is never satisfied",
        )

    @parameterized.expand(ITERABLE_TYPES)
    def test_askip(self, itype: IterableType) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "`count` must be >= 0 but got -1",
            msg="`askip` must raise ValueError if `count` is negative",
        ):
            Stream(src).askip(-1)

        self.assertListEqual(
            to_list(Stream(src).askip(), itype=itype),
            list(src),
            msg="`askip` must be no-op if both `count` and `until` are None",
        )

        self.assertListEqual(
            to_list(Stream(src).askip(None), itype=itype),
            list(src),
            msg="`askip` must be no-op if both `count` and `until` are None",
        )

        for count in [0, 1, 3]:
            self.assertListEqual(
                to_list(Stream(src).askip(count), itype=itype),
                list(src)[count:],
                msg="`askip` must skip `count` elements",
            )

            self.assertListEqual(
                to_list(
                    Stream(map(throw_for_odd_func(TestError), src))
                    .askip(count)
                    .catch(TestError),
                    itype=itype,
                ),
                list(filter(lambda i: i % 2 == 0, src))[count:],
                msg="`askip` must not count exceptions as skipped elements",
            )

            self.assertListEqual(
                to_list(
                    Stream(src).askip(until=asyncify(lambda n: n >= count)), itype=itype
                ),
                list(src)[count:],
                msg="`askip` must yield starting from the first element satisfying `until`",
            )

            self.assertListEqual(
                to_list(
                    Stream(src).askip(count, until=asyncify(lambda n: False)),
                    itype=itype,
                ),
                list(src)[count:],
                msg="`askip` must ignore `count` elements if `until` is never satisfied",
            )

            self.assertListEqual(
                to_list(
                    Stream(src).askip(count * 2, until=asyncify(lambda n: n >= count)),
                    itype=itype,
                ),
                list(src)[count:],
                msg="`askip` must ignore less than `count` elements if `until` is satisfied first",
            )

        self.assertListEqual(
            to_list(Stream(src).askip(until=asyncify(lambda n: False)), itype=itype),
            [],
            msg="`askip` must not yield any element if `until` is never satisfied",
        )

    @parameterized.expand(ITERABLE_TYPES)
    def test_truncate(self, itype: IterableType) -> None:
        self.assertListEqual(
            to_list(Stream(src).truncate(N * 2), itype=itype),
            list(src),
            msg="`truncate` must be ok with count >= stream length",
        )

        self.assertListEqual(
            to_list(Stream(src).truncate(), itype=itype),
            list(src),
            msg="`truncate must be no-op if both `count` and `when` are None",
        )

        self.assertListEqual(
            to_list(Stream(src).truncate(None), itype=itype),
            list(src),
            msg="`truncate must be no-op if both `count` and `when` are None",
        )

        self.assertListEqual(
            to_list(Stream(src).truncate(2), itype=itype),
            [0, 1],
            msg="`truncate` must be ok with count >= 1",
        )
        self.assertListEqual(
            to_list(Stream(src).truncate(1), itype=itype),
            [0],
            msg="`truncate` must be ok with count == 1",
        )
        self.assertListEqual(
            to_list(Stream(src).truncate(0), itype=itype),
            [],
            msg="`truncate` must be ok with count == 0",
        )

        with self.assertRaisesRegex(
            ValueError,
            "`count` must be >= 0 but got -1",
            msg="`truncate` must raise ValueError if `count` is negative",
        ):
            Stream(src).truncate(-1)

        self.assertListEqual(
            to_list(Stream(src).truncate(cast(int, float("inf"))), itype=itype),
            list(src),
            msg="`truncate` must be no-op if `count` is inf",
        )

        count = N // 2
        raising_stream_iterator = bi_iterable_to_iter(
            Stream(lambda: map(lambda x: round((1 / x) * x**2), src)).truncate(count),
            itype=itype,
        )

        with self.assertRaises(
            ZeroDivisionError,
            msg="`truncate` must not stop iteration when encountering exceptions and raise them without counting them...",
        ):
            anext_or_next(raising_stream_iterator)

        self.assertListEqual(
            alist_or_list(raising_stream_iterator), list(range(1, count + 1))
        )

        with self.assertRaises(
            stopiteration_for_iter_type(type(raising_stream_iterator)),
            msg="... and after reaching the limit it still continues to raise StopIteration on calls to next",
        ):
            anext_or_next(raising_stream_iterator)

        iter_truncated_on_predicate = bi_iterable_to_iter(
            Stream(src).truncate(when=lambda n: n == 5), itype=itype
        )
        self.assertListEqual(
            alist_or_list(iter_truncated_on_predicate),
            to_list(Stream(src).truncate(5), itype=itype),
            msg="`when` n == 5 must be equivalent to `count` = 5",
        )
        with self.assertRaises(
            stopiteration_for_iter_type(type(iter_truncated_on_predicate)),
            msg="After exhaustion a call to __next__ on a truncated iterator must raise StopIteration",
        ):
            anext_or_next(iter_truncated_on_predicate)

        with self.assertRaises(
            ZeroDivisionError,
            msg="an exception raised by `when` must be raised",
        ):
            to_list(Stream(src).truncate(when=lambda _: 1 / 0), itype=itype)

        self.assertListEqual(
            to_list(Stream(src).truncate(6, when=lambda n: n == 5), itype=itype),
            list(range(5)),
            msg="`when` and `count` argument can be set at the same time, and the truncation should happen as soon as one or the other is satisfied.",
        )

        self.assertListEqual(
            to_list(Stream(src).truncate(5, when=lambda n: n == 6), itype=itype),
            list(range(5)),
            msg="`when` and `count` argument can be set at the same time, and the truncation should happen as soon as one or the other is satisfied.",
        )

    @parameterized.expand(ITERABLE_TYPES)
    def test_atruncate(self, itype: IterableType) -> None:
        self.assertListEqual(
            to_list(Stream(src).atruncate(N * 2), itype=itype),
            list(src),
            msg="`atruncate` must be ok with count >= stream length",
        )

        self.assertListEqual(
            to_list(Stream(src).atruncate(), itype=itype),
            list(src),
            msg="`atruncate` must be no-op if both `count` and `when` are None",
        )

        self.assertListEqual(
            to_list(Stream(src).atruncate(None), itype=itype),
            list(src),
            msg="`atruncate` must be no-op if both `count` and `when` are None",
        )

        self.assertListEqual(
            to_list(Stream(src).atruncate(2), itype=itype),
            [0, 1],
            msg="`atruncate` must be ok with count >= 1",
        )
        self.assertListEqual(
            to_list(Stream(src).atruncate(1), itype=itype),
            [0],
            msg="`atruncate` must be ok with count == 1",
        )
        self.assertListEqual(
            to_list(Stream(src).atruncate(0), itype=itype),
            [],
            msg="`atruncate` must be ok with count == 0",
        )

        with self.assertRaisesRegex(
            ValueError,
            "`count` must be >= 0 but got -1",
            msg="`atruncate` must raise ValueError if `count` is negative",
        ):
            Stream(src).atruncate(-1)

        self.assertListEqual(
            to_list(Stream(src).atruncate(cast(int, float("inf"))), itype=itype),
            list(src),
            msg="`atruncate` must be no-op if `count` is inf",
        )

        count = N // 2
        raising_stream_iterator = bi_iterable_to_iter(
            Stream(lambda: map(lambda x: round((1 / x) * x**2), src)).atruncate(count),
            itype=itype,
        )

        with self.assertRaises(
            ZeroDivisionError,
            msg="`atruncate` must not stop iteration when encountering exceptions and raise them without counting them...",
        ):
            anext_or_next(raising_stream_iterator)

        self.assertListEqual(
            alist_or_list(raising_stream_iterator), list(range(1, count + 1))
        )

        with self.assertRaises(
            stopiteration_for_iter_type(type(raising_stream_iterator)),
            msg="... and after reaching the limit it still continues to raise StopIteration on calls to next",
        ):
            anext_or_next(raising_stream_iterator)

        iter_truncated_on_predicate = bi_iterable_to_iter(
            Stream(src).atruncate(when=asyncify(lambda n: n == 5)), itype=itype
        )
        self.assertListEqual(
            alist_or_list(iter_truncated_on_predicate),
            to_list(Stream(src).atruncate(5), itype=itype),
            msg="`when` n == 5 must be equivalent to `count` = 5",
        )
        with self.assertRaises(
            stopiteration_for_iter_type(type(iter_truncated_on_predicate)),
            msg="After exhaustion a call to __next__ on a truncated iterator must raise StopIteration",
        ):
            anext_or_next(iter_truncated_on_predicate)

        with self.assertRaises(
            ZeroDivisionError,
            msg="an exception raised by `when` must be raised",
        ):
            to_list(Stream(src).atruncate(when=asyncify(lambda _: 1 / 0)), itype=itype)

        self.assertListEqual(
            to_list(
                Stream(src).atruncate(6, when=asyncify(lambda n: n == 5)), itype=itype
            ),
            list(range(5)),
            msg="`when` and `count` argument can be set at the same time, and the truncation should happen as soon as one or the other is satisfied.",
        )

        self.assertListEqual(
            to_list(
                Stream(src).atruncate(5, when=asyncify(lambda n: n == 6)), itype=itype
            ),
            list(range(5)),
            msg="`when` and `count` argument can be set at the same time, and the truncation should happen as soon as one or the other is satisfied.",
        )

    @parameterized.expand(ITERABLE_TYPES)
    def test_group(self, itype: IterableType) -> None:
        # behavior with invalid arguments
        for seconds in [-1, 0]:
            with self.assertRaises(
                ValueError,
                msg="`group` should raise error when called with `seconds` <= 0.",
            ):
                to_list(
                    Stream([1]).group(
                        size=100, interval=datetime.timedelta(seconds=seconds)
                    ),
                    itype=itype,
                )

        for size in [-1, 0]:
            with self.assertRaises(
                ValueError,
                msg="`group` should raise error when called with `size` < 1.",
            ):
                to_list(Stream([1]).group(size=size), itype=itype)

        # group size
        self.assertListEqual(
            to_list(Stream(range(6)).group(size=4), itype=itype),
            [[0, 1, 2, 3], [4, 5]],
            msg="",
        )
        self.assertListEqual(
            to_list(Stream(range(6)).group(size=2), itype=itype),
            [[0, 1], [2, 3], [4, 5]],
            msg="",
        )
        self.assertListEqual(
            to_list(Stream([]).group(size=2), itype=itype),
            [],
            msg="",
        )

        # behavior with exceptions
        def f(i):
            return i / (110 - i)

        stream_iterator = bi_iterable_to_iter(
            Stream(lambda: map(f, src)).group(100), itype=itype
        )
        anext_or_next(stream_iterator)
        self.assertListEqual(
            anext_or_next(stream_iterator),
            list(map(f, range(100, 110))),
            msg="when encountering upstream exception, `group` should yield the current accumulated group...",
        )

        with self.assertRaises(
            ZeroDivisionError,
            msg="... and raise the upstream exception during the next call to `next`...",
        ):
            anext_or_next(stream_iterator)

        self.assertListEqual(
            anext_or_next(stream_iterator),
            list(map(f, range(111, 211))),
            msg="... and restarting a fresh group to yield after that.",
        )

        # behavior of the `seconds` parameter
        self.assertListEqual(
            to_list(
                Stream(lambda: map(slow_identity, src)).group(
                    size=100,
                    interval=datetime.timedelta(seconds=slow_identity_duration / 1000),
                ),
                itype=itype,
            ),
            list(map(lambda e: [e], src)),
            msg="`group` should not yield empty groups even though `interval` if smaller than upstream's frequency",
        )
        self.assertListEqual(
            to_list(
                Stream(lambda: map(slow_identity, src)).group(
                    size=100,
                    interval=datetime.timedelta(seconds=slow_identity_duration / 1000),
                    by=lambda _: None,
                ),
                itype=itype,
            ),
            list(map(lambda e: [e], src)),
            msg="`group` with `by` argument should not yield empty groups even though `interval` if smaller than upstream's frequency",
        )
        self.assertListEqual(
            to_list(
                Stream(lambda: map(slow_identity, src)).group(
                    size=100,
                    interval=datetime.timedelta(
                        seconds=2 * slow_identity_duration * 0.99
                    ),
                ),
                itype=itype,
            ),
            list(map(lambda e: [e, e + 1], even_src)),
            msg="`group` should yield upstream elements in a two-element group if `interval` inferior to twice the upstream yield period",
        )

        self.assertListEqual(
            anext_or_next(bi_iterable_to_iter(Stream(src).group(), itype=itype)),
            list(src),
            msg="`group` without arguments should group the elements all together",
        )

        # test agroupby
        groupby_stream_iter: Union[
            Iterator[Tuple[int, List[int]]], AsyncIterator[Tuple[int, List[int]]]
        ] = bi_iterable_to_iter(
            Stream(src).groupby(lambda n: n % 2, size=2), itype=itype
        )
        self.assertListEqual(
            [anext_or_next(groupby_stream_iter), anext_or_next(groupby_stream_iter)],
            [(0, [0, 2]), (1, [1, 3])],
            msg="`groupby` must cogroup elements.",
        )

        # test by
        stream_iter = bi_iterable_to_iter(
            Stream(src).group(size=2, by=lambda n: n % 2), itype=itype
        )
        self.assertListEqual(
            [anext_or_next(stream_iter), anext_or_next(stream_iter)],
            [[0, 2], [1, 3]],
            msg="`group` called with a `by` function must cogroup elements.",
        )

        self.assertListEqual(
            anext_or_next(
                bi_iterable_to_iter(
                    Stream(src_raising_at_exhaustion).group(
                        size=10, by=lambda n: n % 4 != 0
                    ),
                    itype=itype,
                ),
            ),
            [1, 2, 3, 5, 6, 7, 9, 10, 11, 13],
            msg="`group` called with a `by` function and a `size` should yield the first batch becoming full.",
        )

        self.assertListEqual(
            to_list(Stream(src).group(by=lambda n: n % 2), itype=itype),
            [list(range(0, N, 2)), list(range(1, N, 2))],
            msg="`group` called with a `by` function and an infinite size must cogroup elements and yield groups starting with the group containing the oldest element.",
        )

        self.assertListEqual(
            to_list(Stream(range(10)).group(by=lambda n: n % 4 == 0), itype=itype),
            [[0, 4, 8], [1, 2, 3, 5, 6, 7, 9]],
            msg="`group` called with a `by` function and reaching exhaustion must cogroup elements and yield uncomplete groups starting with the group containing the oldest element, even though it's not the largest.",
        )

        stream_iter = bi_iterable_to_iter(
            Stream(src_raising_at_exhaustion).group(by=lambda n: n % 2), itype=itype
        )
        self.assertListEqual(
            [anext_or_next(stream_iter), anext_or_next(stream_iter)],
            [list(range(0, N, 2)), list(range(1, N, 2))],
            msg="`group` called with a `by` function and encountering an exception must cogroup elements and yield uncomplete groups starting with the group containing the oldest element.",
        )
        with self.assertRaises(
            TestError,
            msg="`group` called with a `by` function and encountering an exception must raise it after all groups have been yielded",
        ):
            anext_or_next(stream_iter)

        # test seconds + by
        self.assertListEqual(
            to_list(
                Stream(lambda: map(slow_identity, range(10))).group(
                    interval=datetime.timedelta(seconds=slow_identity_duration * 2.9),
                    by=lambda n: n % 4 == 0,
                ),
                itype=itype,
            ),
            [[1, 2], [0, 4], [3, 5, 6, 7], [8], [9]],
            msg="`group` called with a `by` function must cogroup elements and yield the largest groups when `seconds` is reached event though it's not the oldest.",
        )

        stream_iter = bi_iterable_to_iter(
            Stream(src).group(
                size=3,
                by=lambda n: throw(stopiteration_for_iter_type(itype)) if n == 2 else n,
            ),
            itype=itype,
        )
        self.assertListEqual(
            [anext_or_next(stream_iter), anext_or_next(stream_iter)],
            [[0], [1]],
            msg="`group` should yield incomplete groups when `by` raises",
        )
        with self.assertRaisesRegex(
            (WrappedError, RuntimeError),
            stopiteration_for_iter_type(itype).__name__,
            msg="`group` should raise and skip `elem` if `by(elem)` raises",
        ):
            anext_or_next(stream_iter)
        self.assertListEqual(
            anext_or_next(stream_iter),
            [3],
            msg="`group` should continue yielding after `by`'s exception has been raised.",
        )

    @parameterized.expand(ITERABLE_TYPES)
    def test_agroup(self, itype: IterableType) -> None:
        # behavior with invalid arguments
        for seconds in [-1, 0]:
            with self.assertRaises(
                ValueError,
                msg="`agroup` should raise error when called with `seconds` <= 0.",
            ):
                to_list(
                    Stream([1]).agroup(
                        size=100, interval=datetime.timedelta(seconds=seconds)
                    ),
                    itype=itype,
                )

        for size in [-1, 0]:
            with self.assertRaises(
                ValueError,
                msg="`agroup` should raise error when called with `size` < 1.",
            ):
                to_list(Stream([1]).agroup(size=size), itype=itype)

        # group size
        self.assertListEqual(
            to_list(Stream(range(6)).agroup(size=4), itype=itype),
            [[0, 1, 2, 3], [4, 5]],
            msg="",
        )
        self.assertListEqual(
            to_list(Stream(range(6)).agroup(size=2), itype=itype),
            [[0, 1], [2, 3], [4, 5]],
            msg="",
        )
        self.assertListEqual(
            to_list(Stream([]).agroup(size=2), itype=itype),
            [],
            msg="",
        )

        # behavior with exceptions
        def f(i):
            return i / (110 - i)

        stream_iterator = bi_iterable_to_iter(
            Stream(lambda: map(f, src)).agroup(100), itype=itype
        )
        anext_or_next(stream_iterator)
        self.assertListEqual(
            anext_or_next(stream_iterator),
            list(map(f, range(100, 110))),
            msg="when encountering upstream exception, `agroup` should yield the current accumulated group...",
        )

        with self.assertRaises(
            ZeroDivisionError,
            msg="... and raise the upstream exception during the next call to `next`...",
        ):
            anext_or_next(stream_iterator)

        self.assertListEqual(
            anext_or_next(stream_iterator),
            list(map(f, range(111, 211))),
            msg="... and restarting a fresh group to yield after that.",
        )

        # behavior of the `seconds` parameter
        self.assertListEqual(
            to_list(
                Stream(lambda: map(slow_identity, src)).agroup(
                    size=100,
                    interval=datetime.timedelta(seconds=slow_identity_duration / 1000),
                ),
                itype=itype,
            ),
            list(map(lambda e: [e], src)),
            msg="`agroup` should not yield empty groups even though `interval` if smaller than upstream's frequency",
        )
        self.assertListEqual(
            to_list(
                Stream(lambda: map(slow_identity, src)).agroup(
                    size=100,
                    interval=datetime.timedelta(seconds=slow_identity_duration / 1000),
                    by=asyncify(lambda _: None),
                ),
                itype=itype,
            ),
            list(map(lambda e: [e], src)),
            msg="`agroup` with `by` argument should not yield empty groups even though `interval` if smaller than upstream's frequency",
        )
        self.assertListEqual(
            to_list(
                Stream(lambda: map(slow_identity, src)).agroup(
                    size=100,
                    interval=datetime.timedelta(
                        seconds=2 * slow_identity_duration * 0.99
                    ),
                ),
                itype=itype,
            ),
            list(map(lambda e: [e, e + 1], even_src)),
            msg="`agroup` should yield upstream elements in a two-element group if `interval` inferior to twice the upstream yield period",
        )

        self.assertListEqual(
            anext_or_next(bi_iterable_to_iter(Stream(src).agroup(), itype=itype)),
            list(src),
            msg="`agroup` without arguments should group the elements all together",
        )

        # test agroupby
        groupby_stream_iter: Union[
            Iterator[Tuple[int, List[int]]], AsyncIterator[Tuple[int, List[int]]]
        ] = bi_iterable_to_iter(
            Stream(src).agroupby(asyncify(lambda n: n % 2), size=2), itype=itype
        )
        self.assertListEqual(
            [anext_or_next(groupby_stream_iter), anext_or_next(groupby_stream_iter)],
            [(0, [0, 2]), (1, [1, 3])],
            msg="`agroupby` must cogroup elements.",
        )

        # test by
        stream_iter = bi_iterable_to_iter(
            Stream(src).agroup(size=2, by=asyncify(lambda n: n % 2)), itype=itype
        )
        self.assertListEqual(
            [anext_or_next(stream_iter), anext_or_next(stream_iter)],
            [[0, 2], [1, 3]],
            msg="`agroup` called with a `by` function must cogroup elements.",
        )

        self.assertListEqual(
            anext_or_next(
                bi_iterable_to_iter(
                    Stream(src_raising_at_exhaustion).agroup(
                        size=10, by=asyncify(lambda n: n % 4 != 0)
                    ),
                    itype=itype,
                ),
            ),
            [1, 2, 3, 5, 6, 7, 9, 10, 11, 13],
            msg="`agroup` called with a `by` function and a `size` should yield the first batch becoming full.",
        )

        self.assertListEqual(
            to_list(Stream(src).agroup(by=asyncify(lambda n: n % 2)), itype=itype),
            [list(range(0, N, 2)), list(range(1, N, 2))],
            msg="`agroup` called with a `by` function and an infinite size must cogroup elements and yield groups starting with the group containing the oldest element.",
        )

        self.assertListEqual(
            to_list(
                Stream(range(10)).agroup(by=asyncify(lambda n: n % 4 == 0)), itype=itype
            ),
            [[0, 4, 8], [1, 2, 3, 5, 6, 7, 9]],
            msg="`agroup` called with a `by` function and reaching exhaustion must cogroup elements and yield uncomplete groups starting with the group containing the oldest element, even though it's not the largest.",
        )

        stream_iter = bi_iterable_to_iter(
            Stream(src_raising_at_exhaustion).agroup(by=asyncify(lambda n: n % 2)),
            itype=itype,
        )
        self.assertListEqual(
            [anext_or_next(stream_iter), anext_or_next(stream_iter)],
            [list(range(0, N, 2)), list(range(1, N, 2))],
            msg="`agroup` called with a `by` function and encountering an exception must cogroup elements and yield uncomplete groups starting with the group containing the oldest element.",
        )
        with self.assertRaises(
            TestError,
            msg="`agroup` called with a `by` function and encountering an exception must raise it after all groups have been yielded",
        ):
            anext_or_next(stream_iter)

        # test seconds + by
        self.assertListEqual(
            to_list(
                Stream(lambda: map(slow_identity, range(10))).agroup(
                    interval=datetime.timedelta(seconds=slow_identity_duration * 2.9),
                    by=asyncify(lambda n: n % 4 == 0),
                ),
                itype=itype,
            ),
            [[1, 2], [0, 4], [3, 5, 6, 7], [8], [9]],
            msg="`agroup` called with a `by` function must cogroup elements and yield the largest groups when `seconds` is reached event though it's not the oldest.",
        )

        stream_iter = bi_iterable_to_iter(
            Stream(src).agroup(
                size=3,
                by=asyncify(
                    lambda n: throw(stopiteration_for_iter_type(itype)) if n == 2 else n
                ),
            ),
            itype=itype,
        )
        self.assertListEqual(
            [anext_or_next(stream_iter), anext_or_next(stream_iter)],
            [[0], [1]],
            msg="`agroup` should yield incomplete groups when `by` raises",
        )
        with self.assertRaisesRegex(
            (WrappedError, RuntimeError),
            stopiteration_for_iter_type(itype).__name__,
            msg="`agroup` should raise and skip `elem` if `by(elem)` raises",
        ):
            anext_or_next(stream_iter)
        self.assertListEqual(
            anext_or_next(stream_iter),
            [3],
            msg="`agroup` should continue yielding after `by`'s exception has been raised.",
        )

    @parameterized.expand(ITERABLE_TYPES)
    def test_throttle(self, itype: IterableType) -> None:
        # behavior with invalid arguments
        with self.assertRaisesRegex(
            ValueError,
            r"`per` must be None or a positive timedelta but got datetime\.timedelta\(0\)",
            msg="`throttle` should raise error when called with negative `per`.",
        ):
            to_list(
                Stream([1]).throttle(1, per=datetime.timedelta(microseconds=0)),
                itype=itype,
            )
        with self.assertRaisesRegex(
            ValueError,
            "`count` must be >= 1 but got 0",
            msg="`throttle` should raise error when called with `count` < 1.",
        ):
            to_list(
                Stream([1]).throttle(0, per=datetime.timedelta(seconds=1)), itype=itype
            )

        # test interval
        interval_seconds = 0.3
        super_slow_elem_pull_seconds = 2 * interval_seconds
        N = 10
        integers = range(N)

        def slow_first_elem(elem: int):
            if elem == 0:
                time.sleep(super_slow_elem_pull_seconds)
            return elem

        for stream, expected_elems in cast(
            List[Tuple[Stream, List]],
            [
                (
                    Stream(map(slow_first_elem, integers)).throttle(
                        1, per=datetime.timedelta(seconds=interval_seconds)
                    ),
                    list(integers),
                ),
                (
                    Stream(map(throw_func(TestError), map(slow_first_elem, integers)))
                    .throttle(1, per=datetime.timedelta(seconds=interval_seconds))
                    .catch(TestError),
                    [],
                ),
            ],
        ):
            with self.subTest(stream=stream):
                duration, res = timestream(stream, itype=itype)
                self.assertListEqual(
                    res,
                    expected_elems,
                    msg="`throttle` with `interval` must yield upstream elements",
                )
                expected_duration = (
                    N - 1
                ) * interval_seconds + super_slow_elem_pull_seconds
                self.assertAlmostEqual(
                    duration,
                    expected_duration,
                    delta=0.1 * expected_duration,
                    msg="avoid bursts after very slow particular upstream elements",
                )

        self.assertEqual(
            anext_or_next(
                bi_iterable_to_iter(
                    Stream(src)
                    .throttle(1, per=datetime.timedelta(seconds=0.2))
                    .throttle(1, per=datetime.timedelta(seconds=0.1)),
                    itype=itype,
                )
            ),
            0,
            msg="`throttle` should avoid 'ValueError: sleep length must be non-negative' when upstream is slower than `interval`",
        )

        # test per_second

        for N in [1, 10, 11]:
            integers = range(N)
            per_second = 2
            for stream, expected_elems in cast(
                List[Tuple[Stream, List]],
                [
                    (
                        Stream(integers).throttle(
                            per_second, per=datetime.timedelta(seconds=1)
                        ),
                        list(integers),
                    ),
                    (
                        Stream(map(throw_func(TestError), integers))
                        .throttle(per_second, per=datetime.timedelta(seconds=1))
                        .catch(TestError),
                        [],
                    ),
                ],
            ):
                with self.subTest(N=N, stream=stream):
                    duration, res = timestream(stream, itype=itype)
                    self.assertListEqual(
                        res,
                        expected_elems,
                        msg="`throttle` with `per_second` must yield upstream elements",
                    )
                    expected_duration = math.ceil(N / per_second) - 1
                    self.assertAlmostEqual(
                        duration,
                        expected_duration,
                        delta=0.01 * expected_duration + 0.01,
                        msg="`throttle` must slow according to `per_second`",
                    )

        # test chain

        expected_duration = 2
        for stream in [
            Stream(range(11))
            .throttle(5, per=datetime.timedelta(seconds=1))
            .throttle(1, per=datetime.timedelta(seconds=0.01)),
            Stream(range(11))
            .throttle(20, per=datetime.timedelta(seconds=1))
            .throttle(1, per=datetime.timedelta(seconds=0.2)),
        ]:
            with self.subTest(stream=stream):
                duration, _ = timestream(stream, itype=itype)
                self.assertAlmostEqual(
                    duration,
                    expected_duration,
                    delta=0.1 * expected_duration,
                    msg="`throttle` with both `per_second` and `interval` set should follow the most restrictive",
                )

        with self.assertRaisesRegex(
            TypeError,
            r"Stream.throttle\(\) got an unexpected keyword argument 'foo'",
            msg="`throttle` must raise for unsupported kwarg",
        ):
            Stream(src).throttle(foo="bar")

        self.assertEqual(
            Stream(src)
            .throttle(per_second=2)
            .throttle(per_minute=3)
            .throttle(per_hour=4)
            .throttle(interval=datetime.timedelta(milliseconds=1)),
            Stream(src)
            .throttle(2, per=datetime.timedelta(seconds=1))
            .throttle(3, per=datetime.timedelta(minutes=1))
            .throttle(4, per=datetime.timedelta(hours=1))
            .throttle(1, per=datetime.timedelta(milliseconds=1)),
            msg="`throttle` must support legacy kwargs",
        )

        self.assertEqual(
            Stream(src).throttle(
                per_second=2,
                per_minute=3,
                per_hour=4,
                interval=datetime.timedelta(milliseconds=1),
            ),
            Stream(src)
            .throttle(2, per=datetime.timedelta(seconds=1))
            .throttle(3, per=datetime.timedelta(minutes=1))
            .throttle(4, per=datetime.timedelta(hours=1))
            .throttle(1, per=datetime.timedelta(milliseconds=1)),
            msg="`throttle` must support legacy kwargs",
        )

        self.assertEqual(
            Stream(src).throttle(
                5,
                per=datetime.timedelta(microseconds=1),
                per_second=2,
                per_minute=3,
                per_hour=4,
                interval=datetime.timedelta(milliseconds=1),
            ),
            Stream(src)
            .throttle(5, per=datetime.timedelta(microseconds=1))
            .throttle(2, per=datetime.timedelta(seconds=1))
            .throttle(3, per=datetime.timedelta(minutes=1))
            .throttle(4, per=datetime.timedelta(hours=1))
            .throttle(1, per=datetime.timedelta(milliseconds=1)),
            msg="`throttle` must support legacy kwargs",
        )

    @parameterized.expand(ITERABLE_TYPES)
    def test_distinct(self, itype: IterableType) -> None:
        self.assertListEqual(
            to_list(Stream("abbcaabcccddd").distinct(), itype=itype),
            list("abcd"),
            msg="`distinct` should yield distinct elements",
        )
        self.assertListEqual(
            to_list(
                Stream("aabbcccaabbcccc").distinct(consecutive_only=True), itype=itype
            ),
            list("abcabc"),
            msg="`distinct` should only remove the duplicates that are consecutive if `consecutive_only=True`",
        )
        for consecutive_only in [True, False]:
            self.assertListEqual(
                to_list(
                    Stream(["foo", "bar", "a", "b"]).distinct(
                        len, consecutive_only=consecutive_only
                    ),
                    itype=itype,
                ),
                ["foo", "a"],
                msg="`distinct` should yield the first encountered elem among duplicates",
            )
            self.assertListEqual(
                to_list(
                    Stream([]).distinct(consecutive_only=consecutive_only), itype=itype
                ),
                [],
                msg="`distinct` should yield zero elements on empty stream",
            )
        with self.assertRaisesRegex(
            TypeError,
            "unhashable type: 'list'",
            msg="`distinct` should raise for non-hashable elements",
        ):
            to_list(Stream([[1]]).distinct(), itype=itype)

    @parameterized.expand(ITERABLE_TYPES)
    def test_adistinct(self, itype: IterableType) -> None:
        self.assertListEqual(
            to_list(Stream("abbcaabcccddd").adistinct(), itype=itype),
            list("abcd"),
            msg="`adistinct` should yield distinct elements",
        )
        self.assertListEqual(
            to_list(
                Stream("aabbcccaabbcccc").adistinct(consecutive_only=True), itype=itype
            ),
            list("abcabc"),
            msg="`adistinct` should only remove the duplicates that are consecutive if `consecutive_only=True`",
        )
        for consecutive_only in [True, False]:
            self.assertListEqual(
                to_list(
                    Stream(["foo", "bar", "a", "b"]).adistinct(
                        asyncify(len), consecutive_only=consecutive_only
                    ),
                    itype=itype,
                ),
                ["foo", "a"],
                msg="`adistinct` should yield the first encountered elem among duplicates",
            )
            self.assertListEqual(
                to_list(
                    Stream([]).adistinct(consecutive_only=consecutive_only), itype=itype
                ),
                [],
                msg="`adistinct` should yield zero elements on empty stream",
            )
        with self.assertRaisesRegex(
            TypeError,
            "unhashable type: 'list'",
            msg="`adistinct` should raise for non-hashable elements",
        ):
            to_list(Stream([[1]]).adistinct(), itype=itype)

    @parameterized.expand(ITERABLE_TYPES)
    def test_catch(self, itype: IterableType) -> None:
        self.assertListEqual(
            to_list(Stream(src).catch(finally_raise=True), itype=itype),
            list(src),
            msg="`catch` should yield elements in exception-less scenarios",
        )

        with self.assertRaisesRegex(
            TypeError,
            "`iterator` must be an Iterator but got a <class 'list'>",
            msg="`catch` function should raise TypeError when first argument is not an Iterator",
        ):
            from streamable.functions import catch

            catch(cast(Iterator[int], [3, 4]), Exception)

        with self.assertRaisesRegex(
            TypeError,
            "`errors` must be None, or a subclass of `Exception`, or an iterable of optional subclasses of `Exception`, but got <class 'int'>",
            msg="`catch` should raise TypeError when first argument is not None or Type[Exception], or Iterable[Optional[Type[Exception]]]",
        ):
            Stream(src).catch(1)  # type: ignore

        def f(i):
            return i / (3 - i)

        stream = Stream(lambda: map(f, src))
        safe_src = list(src)
        del safe_src[3]
        self.assertListEqual(
            to_list(stream.catch(ZeroDivisionError), itype=itype),
            list(map(f, safe_src)),
            msg="If the exception type matches the `error_type`, then the impacted element should be ignored.",
        )
        self.assertListEqual(
            to_list(stream.catch(), itype=itype),
            list(map(f, safe_src)),
            msg="If the predicate is not specified, then all exceptions should be caught.",
        )

        with self.assertRaises(
            ZeroDivisionError,
            msg="If a non caught exception type occurs, then it should be raised.",
        ):
            to_list(stream.catch(TestError), itype=itype)

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
        for caught_erroring_stream in [
            erroring_stream.catch(finally_raise=True),
            erroring_stream.catch(finally_raise=True),
        ]:
            erroring_stream_iterator = bi_iterable_to_iter(
                caught_erroring_stream, itype=itype
            )
            self.assertEqual(
                anext_or_next(erroring_stream_iterator),
                first_value,
                msg="`catch` should yield the first non exception throwing element.",
            )
            n_yields = 1
            with self.assertRaises(
                TestError,
                msg="`catch` should raise the first error encountered when `finally_raise` is True.",
            ):
                while True:
                    anext_or_next(erroring_stream_iterator)
                    n_yields += 1
            with self.assertRaises(
                stopiteration_for_iter_type(type(erroring_stream_iterator)),
                msg="`catch` with `finally_raise`=True should finally raise StopIteration to avoid infinite recursion if there is another catch downstream.",
            ):
                anext_or_next(erroring_stream_iterator)
            self.assertEqual(
                n_yields,
                3,
                msg="3 elements should have passed been yielded between caught exceptions.",
            )

        only_caught_errors_stream = Stream(
            map(lambda _: throw(TestError), range(2000))
        ).catch(TestError)
        self.assertListEqual(
            to_list(only_caught_errors_stream, itype=itype),
            [],
            msg="When upstream raise exceptions without yielding any element, listing the stream must return empty list, without recursion issue.",
        )
        with self.assertRaises(
            stopiteration_for_iter_type(itype),
            msg="When upstream raise exceptions without yielding any element, then the first call to `next` on a stream catching all errors should raise StopIteration.",
        ):
            anext_or_next(bi_iterable_to_iter(only_caught_errors_stream, itype=itype))

        iterator = bi_iterable_to_iter(
            Stream(map(throw, [TestError, ValueError]))
            .catch(ValueError, finally_raise=True)
            .catch(TestError, finally_raise=True),
            itype=itype,
        )
        with self.assertRaises(
            ValueError,
            msg="With 2 chained `catch`s with `finally_raise=True`, the error caught by the first `catch` is finally raised first (even though it was raised second)...",
        ):
            anext_or_next(iterator)
        with self.assertRaises(
            TestError,
            msg="... and then the error caught by the second `catch` is raised...",
        ):
            anext_or_next(iterator)
        with self.assertRaises(
            stopiteration_for_iter_type(type(iterator)),
            msg="... and a StopIteration is raised next.",
        ):
            anext_or_next(iterator)

        with self.assertRaises(
            TypeError,
            msg="`catch` does not catch if `when` not satisfied",
        ):
            to_list(
                Stream(map(throw, [ValueError, TypeError])).catch(
                    when=lambda exception: "ValueError" in repr(exception)
                ),
                itype=itype,
            )

        self.assertListEqual(
            to_list(
                Stream(map(lambda n: 1 / n, [0, 1, 2, 4])).catch(
                    ZeroDivisionError, replacement=float("inf")
                ),
                itype=itype,
            ),
            [float("inf"), 1, 0.5, 0.25],
            msg="`catch` should be able to yield a non-None replacement",
        )
        self.assertListEqual(
            to_list(
                Stream(map(lambda n: 1 / n, [0, 1, 2, 4])).catch(
                    ZeroDivisionError, replacement=cast(float, None)
                ),
                itype=itype,
            ),
            [None, 1, 0.5, 0.25],
            msg="`catch` should be able to yield a None replacement",
        )

        errors_counter: Counter[Type[Exception]] = Counter()

        self.assertListEqual(
            to_list(
                Stream(
                    map(
                        lambda n: 1 / n,  # potential ZeroDivisionError
                        map(
                            throw_for_odd_func(TestError),  # potential TestError
                            map(
                                int,  # potential ValueError
                                "01234foo56789",
                            ),
                        ),
                    )
                ).catch(
                    (ValueError, TestError, ZeroDivisionError),
                    when=lambda err: errors_counter.update([type(err)]) is None,
                ),
                itype=itype,
            ),
            list(map(lambda n: 1 / n, range(2, 10, 2))),
            msg="`catch` should accept multiple types",
        )
        self.assertDictEqual(
            errors_counter,
            {TestError: 5, ValueError: 3, ZeroDivisionError: 1},
            msg="`catch` should accept multiple types",
        )

        # with self.assertRaises(
        #     TypeError,
        #     msg="`catch` without any error type must raise",
        # ):
        #     Stream(src).catch()  # type: ignore

        self.assertEqual(
            to_list(Stream(map(int, "foo")).catch(replacement=0), itype=itype),
            [0] * len("foo"),
            msg="`catch` must catch all errors when no error type provided",
        )
        self.assertEqual(
            to_list(
                Stream(map(int, "foo")).catch(
                    (None, None, ValueError, None), replacement=0
                ),
                itype=itype,
            ),
            [0] * len("foo"),
            msg="`catch` must catch the provided non-None error types",
        )
        with self.assertRaises(
            ValueError,
            msg="`catch` must be noop if error type is None",
        ):
            to_list(Stream(map(int, "foo")).catch(None), itype=itype)
        with self.assertRaises(
            ValueError,
            msg="`catch` must be noop if error types are None",
        ):
            to_list(Stream(map(int, "foo")).catch((None, None, None)), itype=itype)

    @parameterized.expand(ITERABLE_TYPES)
    def test_acatch(self, itype: IterableType) -> None:
        self.assertListEqual(
            to_list(Stream(src).acatch(finally_raise=True), itype=itype),
            list(src),
            msg="`acatch` should yield elements in exception-less scenarios",
        )

        with self.assertRaisesRegex(
            TypeError,
            "`iterator` must be an AsyncIterator but got a <class 'list'>",
            msg="`afunctions.acatch` function should raise TypeError when first argument is not an AsyncIterator",
        ):
            from streamable import afunctions

            afunctions.acatch(cast(AsyncIterator, [3, 4]), Exception)

        with self.assertRaisesRegex(
            TypeError,
            "`errors` must be None, or a subclass of `Exception`, or an iterable of optional subclasses of `Exception`, but got <class 'int'>",
            msg="`acatch` should raise TypeError when first argument is not None or Type[Exception], or Iterable[Optional[Type[Exception]]]",
        ):
            Stream(src).acatch(1)  # type: ignore

        def f(i):
            return i / (3 - i)

        stream = Stream(lambda: map(f, src))
        safe_src = list(src)
        del safe_src[3]
        self.assertListEqual(
            to_list(stream.acatch(ZeroDivisionError), itype=itype),
            list(map(f, safe_src)),
            msg="If the exception type matches the `error_type`, then the impacted element should be ignored.",
        )
        self.assertListEqual(
            to_list(stream.acatch(), itype=itype),
            list(map(f, safe_src)),
            msg="If the predicate is not specified, then all exceptions should be caught.",
        )

        with self.assertRaises(
            ZeroDivisionError,
            msg="If a non caught exception type occurs, then it should be raised.",
        ):
            to_list(stream.acatch(TestError), itype=itype)

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
        for caught_erroring_stream in [
            erroring_stream.acatch(finally_raise=True),
            erroring_stream.acatch(finally_raise=True),
        ]:
            erroring_stream_iterator = bi_iterable_to_iter(
                caught_erroring_stream, itype=itype
            )
            self.assertEqual(
                anext_or_next(erroring_stream_iterator),
                first_value,
                msg="`acatch` should yield the first non exception throwing element.",
            )
            n_yields = 1
            with self.assertRaises(
                TestError,
                msg="`acatch` should raise the first error encountered when `finally_raise` is True.",
            ):
                while True:
                    anext_or_next(erroring_stream_iterator)
                    n_yields += 1
            with self.assertRaises(
                stopiteration_for_iter_type(type(erroring_stream_iterator)),
                msg="`acatch` with `finally_raise`=True should finally raise StopIteration to avoid infinite recursion if there is another catch downstream.",
            ):
                anext_or_next(erroring_stream_iterator)
            self.assertEqual(
                n_yields,
                3,
                msg="3 elements should have passed been yielded between caught exceptions.",
            )

        only_caught_errors_stream = Stream(
            map(lambda _: throw(TestError), range(2000))
        ).acatch(TestError)
        self.assertListEqual(
            to_list(only_caught_errors_stream, itype=itype),
            [],
            msg="When upstream raise exceptions without yielding any element, listing the stream must return empty list, without recursion issue.",
        )
        with self.assertRaises(
            stopiteration_for_iter_type(itype),
            msg="When upstream raise exceptions without yielding any element, then the first call to `next` on a stream catching all errors should raise StopIteration.",
        ):
            anext_or_next(bi_iterable_to_iter(only_caught_errors_stream, itype=itype))

        iterator = bi_iterable_to_iter(
            Stream(map(throw, [TestError, ValueError]))
            .acatch(ValueError, finally_raise=True)
            .acatch(TestError, finally_raise=True),
            itype=itype,
        )
        with self.assertRaises(
            ValueError,
            msg="With 2 chained `acatch`s with `finally_raise=True`, the error caught by the first `acatch` is finally raised first (even though it was raised second)...",
        ):
            anext_or_next(iterator)
        with self.assertRaises(
            TestError,
            msg="... and then the error caught by the second `acatch` is raised...",
        ):
            anext_or_next(iterator)
        with self.assertRaises(
            stopiteration_for_iter_type(type(iterator)),
            msg="... and a StopIteration is raised next.",
        ):
            anext_or_next(iterator)

        with self.assertRaises(
            TypeError,
            msg="`acatch` does not catch if `when` not satisfied",
        ):
            to_list(
                Stream(map(throw, [ValueError, TypeError])).acatch(
                    when=asyncify(lambda exception: "ValueError" in repr(exception))
                ),
                itype=itype,
            )

        self.assertListEqual(
            to_list(
                Stream(map(lambda n: 1 / n, [0, 1, 2, 4])).acatch(
                    ZeroDivisionError, replacement=float("inf")
                ),
                itype=itype,
            ),
            [float("inf"), 1, 0.5, 0.25],
            msg="`acatch` should be able to yield a non-None replacement",
        )
        self.assertListEqual(
            to_list(
                Stream(map(lambda n: 1 / n, [0, 1, 2, 4])).acatch(
                    ZeroDivisionError, replacement=cast(float, None)
                ),
                itype=itype,
            ),
            [None, 1, 0.5, 0.25],
            msg="`acatch` should be able to yield a None replacement",
        )

        errors_counter: Counter[Type[Exception]] = Counter()

        self.assertListEqual(
            to_list(
                Stream(
                    map(
                        lambda n: 1 / n,  # potential ZeroDivisionError
                        map(
                            throw_for_odd_func(TestError),  # potential TestError
                            map(
                                int,  # potential ValueError
                                "01234foo56789",
                            ),
                        ),
                    )
                ).acatch(
                    (ValueError, TestError, ZeroDivisionError),
                    when=asyncify(
                        lambda err: errors_counter.update([type(err)]) is None
                    ),
                ),
                itype=itype,
            ),
            list(map(lambda n: 1 / n, range(2, 10, 2))),
            msg="`acatch` should accept multiple types",
        )
        self.assertDictEqual(
            errors_counter,
            {TestError: 5, ValueError: 3, ZeroDivisionError: 1},
            msg="`acatch` should accept multiple types",
        )

        # with self.assertRaises(
        #     TypeError,
        #     msg="`acatch` without any error type must raise",
        # ):
        #     Stream(src).acatch()  # type: ignore

        self.assertEqual(
            to_list(Stream(map(int, "foo")).acatch(replacement=0), itype=itype),
            [0] * len("foo"),
            msg="`acatch` must catch all errors when no error type provided",
        )
        self.assertEqual(
            to_list(
                Stream(map(int, "foo")).acatch(
                    (None, None, ValueError, None), replacement=0
                ),
                itype=itype,
            ),
            [0] * len("foo"),
            msg="`acatch` must catch the provided non-None error types",
        )
        with self.assertRaises(
            ValueError,
            msg="`acatch` must be noop if error type is None",
        ):
            to_list(Stream(map(int, "foo")).acatch(None), itype=itype)
        with self.assertRaises(
            ValueError,
            msg="`acatch` must be noop if error types are None",
        ):
            to_list(Stream(map(int, "foo")).acatch((None, None, None)), itype=itype)

    @parameterized.expand(ITERABLE_TYPES)
    def test_observe(self, itype: IterableType) -> None:
        value_error_rainsing_stream: Stream[List[int]] = (
            Stream("123--678")
            .throttle(10, per=datetime.timedelta(seconds=1))
            .observe("chars")
            .map(int)
            .observe("ints")
            .group(2)
            .observe("int pairs")
        )

        self.assertListEqual(
            to_list(value_error_rainsing_stream.catch(ValueError), itype=itype),
            [[1, 2], [3], [6, 7], [8]],
            msg="This can break due to `group`/`map`/`catch`, check other breaking tests to determine quickly if it's an issue with `observe`.",
        )

        with self.assertRaises(
            ValueError,
            msg="`observe` should forward-raise exceptions",
        ):
            to_list(value_error_rainsing_stream, itype=itype)

    def test_is_iterable(self) -> None:
        self.assertIsInstance(Stream(src), Iterable)
        self.assertIsInstance(Stream(src), AsyncIterable)

    def test_count(self) -> None:
        l: List[int] = []

        def effect(x: int) -> None:
            nonlocal l
            l.append(x)

        stream = Stream(lambda: map(effect, src))
        self.assertEqual(
            stream.count(),
            N,
            msg="`count` should return the count of elements.",
        )
        self.assertListEqual(
            l, list(src), msg="`count` should iterate over the entire stream."
        )

    def test_acount(self) -> None:
        l: List[int] = []

        def effect(x: int) -> None:
            nonlocal l
            l.append(x)

        stream = Stream(lambda: map(effect, src))
        self.assertEqual(
            asyncio.run(stream.acount()),
            N,
            msg="`count` should return the count of elements.",
        )
        self.assertListEqual(
            l, list(src), msg="`count` should iterate over the entire stream."
        )

    def test_call(self) -> None:
        l: List[int] = []
        stream = Stream(src).map(l.append)
        self.assertIs(
            stream(),
            stream,
            msg="`__call__` should return the stream.",
        )
        self.assertListEqual(
            l,
            list(src),
            msg="`__call__` should exhaust the stream.",
        )

    def test_await(self) -> None:
        l: List[int] = []
        stream = Stream(src).map(l.append)
        self.assertIs(
            asyncio.run(awaitable_to_coroutine(stream)),
            stream,
            msg="`__call__` should return the stream.",
        )
        self.assertListEqual(
            l,
            list(src),
            msg="`__call__` should exhaust the stream.",
        )

    @parameterized.expand(ITERABLE_TYPES)
    def test_multiple_iterations(self, itype: IterableType) -> None:
        stream = Stream(src)
        for _ in range(3):
            self.assertListEqual(
                to_list(stream, itype=itype),
                list(src),
                msg="The first iteration over a stream should yield the same elements as any subsequent iteration on the same stream, even if it is based on a `source` returning an iterator that only support 1 iteration.",
            )

    @parameterized.expand(
        [(concurrency, itype) for concurrency in (1, 100) for itype in ITERABLE_TYPES]
    )
    def test_amap(self, concurrency, itype) -> None:
        self.assertListEqual(
            to_list(
                Stream(src).amap(
                    async_randomly_slowed(async_square), concurrency=concurrency
                ),
                itype=itype,
            ),
            list(map(square, src)),
            msg="At any concurrency the `amap` method should act as the builtin map function, transforming elements while preserving input elements order.",
        )
        stream = Stream(src).amap(identity, concurrency=concurrency)  # type: ignore
        with self.assertRaisesRegex(
            TypeError,
            r"must be an async function i\.e\. a function returning a Coroutine but it returned a <class 'int'>",
            msg="`amap` should raise a TypeError if a non async function is passed to it.",
        ):
            anext_or_next(bi_iterable_to_iter(stream, itype=itype))

    @parameterized.expand(
        [(concurrency, itype) for concurrency in (1, 100) for itype in ITERABLE_TYPES]
    )
    def test_aforeach(self, concurrency, itype) -> None:
        self.assertListEqual(
            to_list(
                Stream(src).aforeach(
                    async_randomly_slowed(async_square), concurrency=concurrency
                ),
                itype=itype,
            ),
            list(src),
            msg="At any concurrency the `foreach` method must preserve input elements order.",
        )
        stream = Stream(src).aforeach(identity)  # type: ignore
        with self.assertRaisesRegex(
            TypeError,
            r"`transformation` must be an async function i\.e\. a function returning a Coroutine but it returned a <class 'int'>",
            msg="`aforeach` should raise a TypeError if a non async function is passed to it.",
        ):
            anext_or_next(bi_iterable_to_iter(stream, itype=itype))

    @parameterized.expand(ITERABLE_TYPES)
    def test_pipe(self, itype: IterableType) -> None:
        def func(
            stream: Stream, *ints: int, **strings: str
        ) -> Tuple[Stream, Tuple[int, ...], Dict[str, str]]:
            return stream, ints, strings

        stream = Stream(src)
        ints = (0, 1, 2, 3)
        strings = {"foo": "bar", "bar": "foo"}

        self.assertTupleEqual(
            stream.pipe(func, *ints, **strings),
            (stream, ints, strings),
            msg="`pipe` should pass the stream and args/kwargs to `func`.",
        )

        self.assertListEqual(
            stream.pipe(to_list, itype=itype),
            to_list(stream, itype=itype),
            msg="`pipe` should be ok without args and kwargs.",
        )

    def test_eq(self) -> None:
        stream = (
            Stream(src)
            .catch((TypeError, ValueError), replacement=2, when=identity)
            .acatch((TypeError, ValueError), replacement=2, when=async_identity)
            .distinct(key=identity)
            .adistinct(key=async_identity)
            .filter(identity)
            .afilter(async_identity)
            .foreach(identity, concurrency=3)
            .aforeach(async_identity, concurrency=3)
            .group(3, by=bool)
            .flatten(concurrency=3)
            .agroup(3, by=async_identity)
            .map(sync_to_async_iter)
            .aflatten(concurrency=3)
            .groupby(bool)
            .agroupby(async_identity)
            .map(identity, via="process")
            .amap(async_identity)
            .observe("foo")
            .skip(3)
            .askip(3)
            .truncate(4)
            .atruncate(4)
            .throttle(1, per=datetime.timedelta(seconds=1))
        )

        self.assertEqual(
            stream,
            Stream(src)
            .catch((TypeError, ValueError), replacement=2, when=identity)
            .acatch((TypeError, ValueError), replacement=2, when=async_identity)
            .distinct(key=identity)
            .adistinct(key=async_identity)
            .filter(identity)
            .afilter(async_identity)
            .foreach(identity, concurrency=3)
            .aforeach(async_identity, concurrency=3)
            .group(3, by=bool)
            .flatten(concurrency=3)
            .agroup(3, by=async_identity)
            .map(sync_to_async_iter)
            .aflatten(concurrency=3)
            .groupby(bool)
            .agroupby(async_identity)
            .map(identity, via="process")
            .amap(async_identity)
            .observe("foo")
            .skip(3)
            .askip(3)
            .truncate(4)
            .atruncate(4)
            .throttle(1, per=datetime.timedelta(seconds=1)),
        )
        self.assertNotEqual(
            stream,
            Stream(list(src))  # not same source
            .catch((TypeError, ValueError), replacement=2, when=identity)
            .acatch((TypeError, ValueError), replacement=2, when=async_identity)
            .distinct(key=identity)
            .adistinct(key=async_identity)
            .filter(identity)
            .afilter(async_identity)
            .foreach(identity, concurrency=3)
            .aforeach(async_identity, concurrency=3)
            .group(3, by=bool)
            .flatten(concurrency=3)
            .agroup(3, by=async_identity)
            .map(sync_to_async_iter)
            .aflatten(concurrency=3)
            .groupby(bool)
            .agroupby(async_identity)
            .map(identity, via="process")
            .amap(async_identity)
            .observe("foo")
            .skip(3)
            .askip(3)
            .truncate(4)
            .atruncate(4)
            .throttle(1, per=datetime.timedelta(seconds=1)),
        )
        self.assertNotEqual(
            stream,
            Stream(src)
            .catch((TypeError, ValueError), replacement=2, when=identity)
            .acatch((TypeError, ValueError), replacement=2, when=async_identity)
            .distinct(key=identity)
            .adistinct(key=async_identity)
            .filter(identity)
            .afilter(async_identity)
            .foreach(identity, concurrency=3)
            .aforeach(async_identity, concurrency=3)
            .group(3, by=bool)
            .flatten(concurrency=3)
            .agroup(3, by=async_identity)
            .map(sync_to_async_iter)
            .aflatten(concurrency=3)
            .groupby(bool)
            .agroupby(async_identity)
            .map(identity, via="process")
            .amap(async_identity)
            .observe("foo")
            .skip(3)
            .askip(3)
            .truncate(4)
            .atruncate(4)
            .throttle(1, per=datetime.timedelta(seconds=2)),  # not the same interval
        )

    @parameterized.expand(ITERABLE_TYPES)
    def test_ref_cycles(self, itype: IterableType) -> None:
        async def async_int(o: Any) -> int:
            return int(o)

        stream = (
            Stream("123_5")
            .amap(async_int)
            .map(str)
            .group(1)
            .groupby(len)
            .catch(finally_raise=True)
        )
        exception: Exception
        try:
            to_list(stream, itype=itype)
        except ValueError as e:
            exception = e
        self.assertIsInstance(
            exception,
            ValueError,
            msg="`finally_raise` must be respected",
        )
        self.assertFalse(
            [
                (var, val)
                # go through the frames of the exception's traceback
                for frame, _ in traceback.walk_tb(exception.__traceback__)
                # skipping the current frame
                if frame is not cast(TracebackType, exception.__traceback__).tb_frame
                # go through the locals captured in that frame
                for var, val in frame.f_locals.items()
                # check if one of them is an exception
                if isinstance(val, Exception)
                # check if it is captured in its own traceback
                and frame is cast(TracebackType, val.__traceback__).tb_frame
            ],
            msg=f"the exception's traceback should not contain an exception captured in its own traceback",
        )

    def test_on_queue_in_thread(self) -> None:
        zeros: List[str] = []
        src: "queue.Queue[Optional[str]]" = queue.Queue()
        thread = threading.Thread(
            target=Stream(iter(src.get, None)).foreach(zeros.append)
        )
        thread.start()
        src.put("foo")
        src.put("bar")
        src.put(None)
        thread.join()
        self.assertListEqual(
            zeros,
            ["foo", "bar"],
            msg="stream must work on Queue",
        )

    def test_deepcopy(self) -> None:
        stream = Stream([]).map(str)
        stream_copy = copy.deepcopy(stream)
        self.assertEqual(
            stream,
            stream_copy,
            msg="the copy must be equal",
        )
        self.assertIsNot(
            stream,
            stream_copy,
            msg="the copy must be a different object",
        )
        self.assertIsNot(
            stream.source,
            stream_copy.source,
            msg="the copy's source must be a different object",
        )

    def test_slots(self) -> None:
        stream = Stream(src).filter()
        with self.assertRaises(
            AttributeError,
            msg="a stream should not have a __dict__",
        ):
            Stream(src).__dict__

        self.assertTupleEqual(
            stream.__slots__,
            ("_upstream", "_when"),
            msg="a stream should have __slots__",
        )
