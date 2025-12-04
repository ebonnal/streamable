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
    # should be raising TypeError for non-int concurrency.
    with pytest.raises(TypeError):
        method(stream, *args, concurrency="1")
    # should be raising ValueError for concurrency=0.
    with pytest.raises(ValueError):
        method(stream, *args, concurrency=0)


@pytest.mark.parametrize("method", (Stream.map, Stream.do))
def test_validate_via(method) -> None:
    # must raise a TypeError for invalid via
    with pytest.raises(
        TypeError,
        match="`via` must be 'thread' or 'process' but got 'foo'",
    ):
        method(Stream(src), identity, via="foo")
