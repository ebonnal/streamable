import unittest
from typing import Callable, Iterable, Iterator, List, TypeVar

from streamable.functions import batch, catch, flatten, map, observe, slow

T = TypeVar("T")


def identity(x: T) -> T:
    return x


# size of the test collections
N = 256
src: Callable[[], Iterable[int]] = range(N).__iter__


class TestFunctions(unittest.TestCase):
    def test_signatures(self) -> None:
        it = iter(src())
        mapped_it_1: Iterator[int] = map(identity, it)
        mapped_it_2: Iterator[int] = map(identity, it, concurrency=1)
        mapped_it_3: Iterator[int] = map(identity, it, concurrency=2)
        batched_it_1: Iterator[List[int]] = batch(it, size=1)
        batched_it_2: Iterator[List[int]] = batch(it, size=1, seconds=0.1)
        batched_it_3: Iterator[List[int]] = batch(it, size=1, seconds=2)
        flattened_batched_it_1: Iterator[int] = flatten(batched_it_1)
        flattened_batched_it_2: Iterator[int] = flatten(batched_it_1, concurrency=1)
        flattened_batched_it_3: Iterator[int] = flatten(batched_it_1, concurrency=2)
        catched_it_1: Iterator[int] = catch(it, Exception)
        catched_it_2: Iterator[int] = catch(
            it, Exception, when=lambda e: True, raise_at_exhaustion=True
        )
        observed_it_1: Iterator[int] = observe(it, what="objects")
        observed_it_2: Iterator[int] = observe(it, what="objects", colored=True)
        slowed_it_1: Iterator[int] = slow(it, frequency=1)
