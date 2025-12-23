from typing import Any, Callable, List

import pytest

from streamable import stream
from tests.utils.functions import async_identity, identity
from tests.utils.source import ints_src


@pytest.mark.parametrize(
    "method, args",
    (
        (stream.map, [identity]),
        (stream.map, [async_identity]),
        (stream.do, [identity]),
        (stream.do, [async_identity]),
        (stream.flatten, []),
        (stream.flatten, []),
    ),
)
def test_validate_concurrency(method: Callable[..., Any], args: List[Any]) -> None:
    # should be raising ValueError for concurrency=0.
    with pytest.raises(ValueError, match="`concurrency` must be >= 1 but got 0"):
        method(stream(ints_src), *args, concurrency=0)
