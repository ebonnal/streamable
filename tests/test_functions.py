import asyncio
import datetime
from typing import Callable, Iterator, List, Union, cast

from streamable._functions import (
    catch,
    flatten,
    group,
    map,
    observe,
    throttle,
    truncate,
)


def test_signatures() -> None:
    iterator = iter((0,))
    to = cast(Callable[[int], int], ...)
    mapped_it_1: Iterator[int] = map(to, iterator)  # noqa: F841
    mapped_it_2: Iterator[int] = map(to, iterator, concurrency=1)  # noqa: F841
    mapped_it_3: Iterator[int] = map(to, iterator, concurrency=2)  # noqa: F841
    grouped_it_1: Iterator[List[int]] = group(iterator, up_to=1)
    grouped_it_2: Iterator[List[int]] = group(  # noqa: F841
        iterator, up_to=1, over=datetime.timedelta(seconds=0.1)
    )
    grouped_it_3: Iterator[List[int]] = group(  # noqa: F841
        iterator, up_to=1, over=datetime.timedelta(seconds=2)
    )
    flattened_grouped_it_1: Iterator[int] = flatten(  # noqa: F841
        asyncio.get_running_loop, grouped_it_1
    )
    flattened_grouped_it_2: Iterator[int] = flatten(  # noqa: F841
        asyncio.get_running_loop, grouped_it_1, concurrency=1
    )
    flattened_grouped_it_3: Iterator[int] = flatten(  # noqa: F841
        asyncio.get_running_loop, grouped_it_1, concurrency=2
    )
    caught_it_1: Iterator[int] = catch(iterator, Exception)  # noqa: F841
    caught_it_2: Iterator[int] = catch(iterator, Exception, finally_raise=True)  # noqa: F841
    caught_it_3: Iterator[Union[int, str]] = catch(iterator, Exception, replace=str)  # noqa: F841
    observed_it_1: Iterator[int] = observe(iterator, label="objects")  # noqa: F841
    throttleed_it_1: Iterator[int] = throttle(  # noqa: F841
        iterator,
        1,
        per=datetime.timedelta(seconds=1),
    )
    truncated_it_1: Iterator[int] = truncate(iterator, when=1)  # noqa: F841
