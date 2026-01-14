from concurrent.futures import ProcessPoolExecutor
from typing import Any, Callable, List
import pytest
from streamable import stream
from streamable._tools._func import asyncify
from tests.utils.func import identity
from tests.utils.iter import ITERABLE_TYPES, IterableType, alist_or_list
from tests.utils.source import INTEGERS, ints


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_do_with_process_pool(itype: IterableType) -> None:
    with ProcessPoolExecutor(2) as processes:
        state: List[int] = []
        elements = range(10)
        s = stream(elements).do(state.append, concurrency=processes)
        assert alist_or_list(s, itype) == list(elements)
        assert state == []


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_do(
    itype: IterableType,
    adapt: Callable[[Any], Any],
) -> None:
    state: List[int] = []
    s = ints.do(adapt(state.append), concurrency=2)
    assert alist_or_list(s, itype) == state == list(INTEGERS)
