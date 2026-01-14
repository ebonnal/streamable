from typing import (
    Any,
    Callable,
    Iterable,
    List,
    Tuple,
    cast,
)

import pytest

from streamable import stream
from streamable._tools._iter import async_iter
from tests.utils.func import (
    identity,
)
from tests.utils.iter import (
    ITERABLE_TYPES,
    IterableType,
    alist_or_list,
)


def test_flatten_typing() -> None:
    """Test that flatten works with various iterable types for type checking."""
    flattened_iterator_stream: stream[str] = stream("abc").map(iter).flatten()  # noqa: F841
    flattened_list_stream: stream[str] = stream("abc").map(list).flatten()  # noqa: F841
    flattened_set_stream: stream[str] = stream("abc").map(set).flatten()  # noqa: F841
    flattened_map_stream: stream[str] = (  # noqa: F841
        stream("abc").map(lambda char: map(lambda x: x, char)).flatten()
    )
    flattened_filter_stream: stream[str] = (  # noqa: F841
        stream("abc").map(lambda char: filter(lambda _: True, char)).flatten()
    )

    flattened_asynciter_stream: stream[str] = (  # noqa: F841
        stream("abc").map(iter).map(async_iter).flatten()
    )
    flattened_range_stream: stream[int] = (  # noqa: F841
        stream((INTEGERS, INTEGERS)).flatten()
    )


@pytest.mark.parametrize("to_iter", [identity, async_iter])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_flatten_without_concurrency(
    itype: IterableType, to_iter: Callable[[Any], Any]
) -> None:
    """Flatten with concurrency=1 should yield elements in order of nested for loop."""
    s = stream((range(3), range(3, 6), range(6, 9))).map(to_iter).flatten()
    assert alist_or_list(s, itype) == list(range(9))


@pytest.mark.parametrize("to_iter", [identity, async_iter])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_flatten_with_concurrency(
    itype: IterableType, to_iter: Callable[[Any], Any]
) -> None:
    """Flatten with concurrency=1 should yield elements in order of nested for loop."""
    s = (
        stream(
            (
                range(4),  # 1st
                range(4, 5),  # 2nd
                range(5, 8),  # 3rd
                range(8, 10),  # 4th
            )
        )
        .map(to_iter)
        .flatten(concurrency=2)
    )

    assert alist_or_list(s, itype) == [
        # the 2 iterables currently flattened are 1st and 2nd
        0,  # 1st elem from 1st iterable
        4,  # 1st elem from 2nd iterable
        1,  # 2nd elem from 1st iterable
        # the 2nd iterable exhausts, the 3rd will be enter the flattened next
        2,  # 3rd elem from 1st iterable
        5,  # 1st elem from 3rd iterable
        3,  # 4th elem from 1st iterable
        6,  # 2nd elem from 3rd iterable
        # the 1st iterable exhausts, the 4nd will be enter the flattened
        7,  # 3rd elem from 3rd iterable
        8,  # 1st elem from 4th iterable
        9,  # 2nd elem from 4th iterable
    ]


@pytest.mark.parametrize("concurrency", (1, 2))
@pytest.mark.parametrize("to_iter", (identity, async_iter))
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_flatten_with_exceptions(
    concurrency: int,
    itype: IterableType,
    to_iter: Callable[[Any], Any],
) -> None:
    """Flatten should be resilient to exceptions raised upstream or by `next(elem)`."""
    value_errors: List[Exception] = []
    type_errors: List[Exception] = []
    # '0' will lead to `TypeError`s when flatten calls next on upstream
    # '-' will lead to `ValueError`s when flatten calls next on element
    elements = cast(Tuple[str, ...], ("0-1", "234", "", "", "---", "---", 0, 0, "-5-"))
    s = (
        stream(elements)
        .map(lambda chars: map(int, chars))
        .map(to_iter)
        .flatten(concurrency=concurrency)
        .catch(ValueError, do=value_errors.append)
        .catch(TypeError, do=type_errors.append)
    )
    assert set(alist_or_list(s, itype)) == set(range(6))
    assert len(value_errors) == "".join(filter(None, elements)).count("-")
    assert len(type_errors) == len([c for c in elements if not isinstance(c, Iterable)])
