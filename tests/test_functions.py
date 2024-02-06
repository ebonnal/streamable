import unittest
from typing import Iterable, Iterator, List, TypeVar

from streamable.functions import batch, catch, flatten, limit, map, observe, slow

T = TypeVar("T")


def identity(x: T) -> T:
    return x


# size of the test collections
N = 256


def src() -> Iterable[int]:
    return range(N)


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
        catched_it_1: Iterator[int] = catch(it, lambda ex: None)
        catched_it_2: Iterator[int] = catch(
            it, lambda ex: None, raise_at_exhaustion=True
        )
        observed_it_1: Iterator[int] = observe(it, what="objects")
        observed_it_2: Iterator[int] = observe(it, what="objects", colored=True)
        slowed_it_1: Iterator[int] = slow(it, frequency=1)
        limited_it_1: Iterator[int] = limit(it, count=1)
