import pytest

from streamable import Stream
from tests.utils import (
    async_identity,
    identity,
    src,
)


@pytest.mark.parametrize(
    "method, args",
    (
        (Stream.map, [identity]),
        (Stream.map, [async_identity]),
        (Stream.do, [identity]),
        (Stream.do, [async_identity]),
        (Stream.flatten, []),
        (Stream.flatten, []),
    ),
)
def test_validate_concurrency(method, args) -> None:
    stream = Stream(src)
    # should be raising ValueError for concurrency=0.
    with pytest.raises(ValueError, match="`concurrency` must be >= 1 but got 0"):
        method(stream, *args, concurrency=0)
