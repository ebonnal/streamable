from typing import List

import pytest

from streamable import stream
from tests.utils.functions import async_slow_identity
from tests.utils.iteration import (
    ITERABLE_TYPES,
    IterableType,
    alist_or_list,
    anext_or_next,
    aiter_or_iter,
    stopiteration_type,
)
from tests.utils.source import INTEGERS


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_buffer_raises_on_invalid_up_to(itype: IterableType) -> None:
    """Buffer should raise ValueError when up_to is less than 1."""
    with pytest.raises(ValueError, match="`up_to` must be >= 0 but got -1"):
        alist_or_list(stream([1]).buffer(-1), itype=itype)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_buffer_preserves_elements(itype: IterableType) -> None:
    """Buffer should yield all upstream elements in order."""
    assert alist_or_list(stream(INTEGERS).buffer(5), itype=itype) == list(INTEGERS)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
@pytest.mark.parametrize("buffer_size", [0, 1, 10])
def test_buffer_size_is_respected(itype: IterableType, buffer_size: int) -> None:
    buffered: List[int] = []
    buffering_ints = (
        stream(INTEGERS)
        .do(buffered.append)
        .buffer(buffer_size)
        .map(async_slow_identity)
    )
    buffering_ints_iter = aiter_or_iter(buffering_ints, itype=itype)
    assert buffered == []
    assert anext_or_next(buffering_ints_iter, itype=itype) == 0
    assert buffered == list(INTEGERS)[: buffer_size + 1]
    assert anext_or_next(buffering_ints_iter, itype=itype) == 1
    assert buffered == list(INTEGERS)[: buffer_size + 2]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
@pytest.mark.parametrize("buffer_size", [3])
def test_buffer_with_exceptions(itype: IterableType, buffer_size: int) -> None:
    buffered: List[int] = []
    buffering_ints = (
        stream("0-23--6")
        .map(int)
        .do(buffered.append)
        .buffer(buffer_size)
        .map(async_slow_identity)
    )
    buffering_ints_iter = aiter_or_iter(buffering_ints, itype=itype)
    assert buffered == []
    assert anext_or_next(buffering_ints_iter, itype=itype) == 0
    assert buffered == [0, 2, 3]
    with pytest.raises(ValueError):
        anext_or_next(buffering_ints_iter, itype=itype)
    assert anext_or_next(buffering_ints_iter, itype=itype) == 2
    assert buffered == [0, 2, 3]
    assert anext_or_next(buffering_ints_iter, itype=itype) == 3
    assert buffered == [0, 2, 3, 6]
    with pytest.raises(ValueError):
        anext_or_next(buffering_ints_iter, itype=itype)
    with pytest.raises(ValueError):
        anext_or_next(buffering_ints_iter, itype=itype)
    assert anext_or_next(buffering_ints_iter, itype=itype) == 6
    assert buffered == [0, 2, 3, 6]
    with pytest.raises(stopiteration_type(itype)):
        anext_or_next(buffering_ints_iter, itype=itype)
