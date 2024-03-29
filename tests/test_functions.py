import unittest
from typing import Callable, Iterable, Iterator, List, TypeVar, cast

from streamable.functions import catch, flatten, group, limit, map, observe, slow

T = TypeVar("T")


# size of the test collections
N = 256


def src() -> Iterable[int]:
    return range(N)


class TestFunctions(unittest.TestCase):
    def test_signatures(self) -> None:
        it = iter(src())
        func = cast(Callable[[int], int], ...)
        mapped_it_1: Iterator[int] = map(func, it)
        mapped_it_2: Iterator[int] = map(func, it, concurrency=1)
        mapped_it_3: Iterator[int] = map(func, it, concurrency=2)
        grouped_it_1: Iterator[List[int]] = group(it, size=1)
        grouped_it_2: Iterator[List[int]] = group(it, size=1, seconds=0.1)
        grouped_it_3: Iterator[List[int]] = group(it, size=1, seconds=2)
        flattened_grouped_it_1: Iterator[int] = flatten(grouped_it_1)
        flattened_grouped_it_2: Iterator[int] = flatten(grouped_it_1, concurrency=1)
        flattened_grouped_it_3: Iterator[int] = flatten(grouped_it_1, concurrency=2)
        catched_it_1: Iterator[int] = catch(it, lambda ex: None)
        catched_it_2: Iterator[int] = catch(
            it, lambda ex: None, raise_at_exhaustion=True
        )
        observed_it_1: Iterator[int] = observe(it, what="objects")
        observed_it_2: Iterator[int] = observe(it, what="objects", colored=True)
        slowed_it_1: Iterator[int] = slow(it, frequency=1)
        limited_it_1: Iterator[int] = limit(it, count=1)
