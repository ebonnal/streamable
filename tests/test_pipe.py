import time
import timeit
import unittest
from typing import Callable, Iterable, Iterator, TypeVar

from parameterized import parameterized  # type: ignore

from kioss import Pipe

T = TypeVar("T")


def timepipe(pipe: Pipe):
    def iterate():
        for _ in pipe:
            pass

    return timeit.timeit(iterate, number=1)


# simulates an I/0 bound function
slow_identity_duration = 0.01


def slow_identity(x: T) -> T:
    time.sleep(slow_identity_duration)
    return x


def identity(x: T) -> T:
    return x


# size of the test collections
N = 32
src: Callable[[], Iterable[int]] = range(N).__iter__


class TestPipe(unittest.TestCase):
    def test_init(self) -> None:
        pipe = Pipe(src)
        self.assertEqual(
            pipe.source,
            src,
            msg="The pipe's `source` must be the source argument.",
        )
        self.assertIsNone(
            pipe.upstream,
            msg="The `upstream` attribute of a base Pipe's instance must be None.",
        )

        with self.assertRaisesRegex(
            TypeError,
            "source must be a callable but got a <class 'range'>",
            msg="Instantiating a Pipe with a source not being a callable must raise TypeError.",
        ):
            Pipe(range(N))  # type: ignore

    def test_explain(self) -> None:
        complex_pipe: Pipe[int] = (
            Pipe(src)
            .filter(lambda _: True)
            .map(lambda _: _)
            .batch(100)
            .observe("batches")
            .flatten(n_threads=4)
            .slow(64)
            .observe("pipe #1 elements")
            .chain(
                Pipe([].__iter__).do(lambda _: None).observe("pipe #2 elements"),
                Pipe([].__iter__).observe("pipe #3 elements"),
            )
            .catch(ValueError, TypeError, when=lambda _: True)
        )
        explanation_1 = complex_pipe.explain()
        explanation_2 = complex_pipe.explain()
        self.assertEqual(
            explanation_1,
            explanation_2,
            msg="Pipe.explain() must be deterministic.",
        )
        colored_explanation = complex_pipe.explain(colored=True)
        self.assertNotEqual(
            explanation_1,
            colored_explanation,
            msg="Pipe.explain(colored=True) must different from non colored one.",
        )
        explanation_3 = complex_pipe.map(str).explain()
        self.assertNotEqual(
            explanation_1,
            explanation_3,
            msg="explanation of different pipes must be different",
        )

        print(colored_explanation)

    def test_iter(self) -> None:
        self.assertIsInstance(
            iter(Pipe(src)),
            Iterator,
            msg="iter(pipe) must return an Iterator",
        )

    def test_add(self) -> None:
        from kioss._pipe import ChainPipe

        pipe = Pipe(src)
        self.assertIsInstance(
            pipe.chain(pipe),
            ChainPipe,
            msg="iter(pipe) must return an Iterator",
        )

    @parameterized.expand(
        [
            [Pipe.map, [identity]],
            [Pipe.do, [identity]],
            [Pipe.flatten, []],
        ]
    )
    def test_sanitize_n_threads(self, method, args) -> None:
        pipe = Pipe(src)
        with self.assertRaises(
            TypeError,
            msg=f"{method} should be raising TypeError for non-int n_threads.",
        ):
            method(pipe, *args, n_threads="1")

        with self.assertRaises(
            ValueError, msg=f"{method} should be raising ValueError for n_threads=0."
        ):
            method(pipe, *args, n_threads=0)

        for n_threads in range(1, 10):
            self.assertIsInstance(
                method(pipe, *args, n_threads=n_threads),
                Pipe,
                msg=f"it must be ok to call {method} with n_threads={n_threads}",
            )
