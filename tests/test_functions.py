import unittest
from typing import Callable, Iterator, List, TypeVar, cast

from streamable.functions import catch, flatten, group, map, observe, slow, truncate

T = TypeVar("T")


# size of the test collections
N = 256


src = range(N)


class TestFunctions(unittest.TestCase):
    def test_signatures(self) -> None:
        iterator = iter(src)
        transformation = cast(Callable[[int], int], ...)
        mapped_it_1: Iterator[int] = map(transformation, iterator)
        mapped_it_2: Iterator[int] = map(transformation, iterator, concurrency=1)
        mapped_it_3: Iterator[int] = map(transformation, iterator, concurrency=2)
        grouped_it_1: Iterator[List[int]] = group(iterator, size=1)
        grouped_it_2: Iterator[List[int]] = group(iterator, size=1, seconds=0.1)
        grouped_it_3: Iterator[List[int]] = group(iterator, size=1, seconds=2)
        flattened_grouped_it_1: Iterator[int] = flatten(grouped_it_1)
        flattened_grouped_it_2: Iterator[int] = flatten(grouped_it_1, concurrency=1)
        flattened_grouped_it_3: Iterator[int] = flatten(grouped_it_1, concurrency=2)
        catched_it_1: Iterator[int] = catch(iterator, Exception)
        catched_it_2: Iterator[int] = catch(iterator, Exception, finally_raise=True)
        observed_it_1: Iterator[int] = observe(iterator, what="objects")
        slowed_it_1: Iterator[int] = slow(iterator, frequency=1)
        truncated_it_1: Iterator[int] = truncate(iterator, count=1)
