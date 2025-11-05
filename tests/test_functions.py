import datetime
from typing import Callable, Iterator, List, cast

from streamable.functions import catch, flatten, group, map, observe, throttle, truncate


def test_signatures() -> None:
    iterator = iter((0,))
    transformation = cast(Callable[[int], int], ...)
    mapped_it_1: Iterator[int] = map(transformation, iterator)  # noqa: F841
    mapped_it_2: Iterator[int] = map(transformation, iterator, concurrency=1)  # noqa: F841
    mapped_it_3: Iterator[int] = map(transformation, iterator, concurrency=2)  # noqa: F841
    grouped_it_1: Iterator[List[int]] = group(iterator, size=1)
    grouped_it_2: Iterator[List[int]] = group(  # noqa: F841
        iterator, size=1, interval=datetime.timedelta(seconds=0.1)
    )
    grouped_it_3: Iterator[List[int]] = group(  # noqa: F841
        iterator, size=1, interval=datetime.timedelta(seconds=2)
    )
    flattened_grouped_it_1: Iterator[int] = flatten(grouped_it_1)  # noqa: F841
    flattened_grouped_it_2: Iterator[int] = flatten(grouped_it_1, concurrency=1)  # noqa: F841
    flattened_grouped_it_3: Iterator[int] = flatten(grouped_it_1, concurrency=2)  # noqa: F841
    caught_it_1: Iterator[int] = catch(iterator, Exception)  # noqa: F841
    caught_it_2: Iterator[int] = catch(iterator, Exception, finally_raise=True)  # noqa: F841
    observed_it_1: Iterator[int] = observe(iterator, what="objects")  # noqa: F841
    throttleed_it_1: Iterator[int] = throttle(  # noqa: F841
        iterator,
        1,
        per=datetime.timedelta(seconds=1),
    )
    truncated_it_1: Iterator[int] = truncate(iterator, count=1)  # noqa: F841
