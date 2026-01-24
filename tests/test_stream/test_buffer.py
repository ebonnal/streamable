from typing import Any, AsyncIterable, Callable, Iterable, List

import pytest

from streamable import stream
from tests.utils.func import async_slow_identity, slow_identity
from tests.utils.iter import (
    ITERABLE_TYPES,
    IterableType,
    alist_or_list,
    anext_or_next,
    aiter_or_iter,
    stopiteration_type,
)
from tests.utils.source import INTEGERS, ints


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_buffer_raises_on_invalid_up_to(itype: IterableType) -> None:
    with pytest.raises(ValueError, match="`up_to` must be >= 0 but got: -1"):
        alist_or_list(stream([1]).buffer(-1), itype)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_buffer_preserves_elements(itype: IterableType) -> None:
    assert alist_or_list(ints.buffer(5), itype) == list(INTEGERS)


@pytest.mark.parametrize("buffer_size", [0, 1, 10])
@pytest.mark.parametrize(
    "itype, slow_identity",
    [(Iterable, slow_identity), (AsyncIterable, async_slow_identity)],
)
def test_buffer_size_is_respected(
    itype: IterableType, buffer_size: int, slow_identity: Callable[..., Any]
) -> None:
    buffered: List[int] = []
    buffering_ints = ints.do(buffered.append).buffer(buffer_size).map(slow_identity)
    buffering_ints_iter = aiter_or_iter(buffering_ints, itype)
    assert buffered == []
    assert anext_or_next(buffering_ints_iter, itype) == 0
    assert buffered == list(INTEGERS)[: buffer_size + 1]
    assert anext_or_next(buffering_ints_iter, itype) == 1
    assert buffered == list(INTEGERS)[: buffer_size + 2]


@pytest.mark.parametrize(
    "itype, slow_identity",
    [(Iterable, slow_identity), (AsyncIterable, async_slow_identity)],
)
def test_buffer_with_exceptions(
    itype: IterableType, slow_identity: Callable[..., Any]
) -> None:
    buffered: List[int] = []
    buffersize = 3
    buffering_ints = (
        stream("0-23--6")
        .map(int)
        .do(buffered.append)
        .buffer(buffersize)
        .map(slow_identity)
    )
    buffering_ints_iter = aiter_or_iter(buffering_ints, itype)
    assert buffered == []
    assert anext_or_next(buffering_ints_iter, itype) == 0
    assert buffered == [0, 2, 3]
    with pytest.raises(ValueError):
        anext_or_next(buffering_ints_iter, itype)
    assert anext_or_next(buffering_ints_iter, itype) == 2
    assert buffered == [0, 2, 3]
    assert anext_or_next(buffering_ints_iter, itype) == 3
    assert buffered == [0, 2, 3, 6]
    with pytest.raises(ValueError):
        anext_or_next(buffering_ints_iter, itype)
    with pytest.raises(ValueError):
        anext_or_next(buffering_ints_iter, itype)
    assert anext_or_next(buffering_ints_iter, itype) == 6
    assert buffered == [0, 2, 3, 6]
    with pytest.raises(stopiteration_type(itype)):
        anext_or_next(buffering_ints_iter, itype)
