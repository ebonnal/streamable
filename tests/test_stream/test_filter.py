import builtins
from typing import Any, Callable

import pytest

from streamable._tools._func import asyncify
from tests.utils.func import identity
from tests.utils.iter import ITERABLE_TYPES, IterableType, alist_or_list
from tests.utils.source import INTEGERS, ints


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_filter(itype: IterableType, adapt: Callable[[Any], Any]) -> None:
    """Filter should behave like `builtins.filter`."""

    def keep(x) -> int:
        return x % 2

    assert alist_or_list(ints.filter(adapt(keep)), itype) == list(
        builtins.filter(keep, INTEGERS)
    )
    assert alist_or_list(ints.filter(), itype) == list(builtins.filter(None, INTEGERS))
