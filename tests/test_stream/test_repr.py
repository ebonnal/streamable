import datetime
from typing import Any


from streamable._stream import stream
from streamable._tools._func import star
from tests.utils.func import async_identity


def test_repr_source_stream() -> None:
    assert repr(stream([0])) == "stream([0])"


def test_repr_one_operation() -> None:
    assert repr(stream([0]).skip(10)) == "stream([0]).skip(until=10)"


def test_repr_multiple_operations() -> None:
    assert (
        repr(stream([0]).skip(10).skip(10))
        == "stream([0]).skip(until=10).skip(until=10)"
    )

    # goes to line when too long
    assert (
        repr(stream([0]).skip(10).skip(10).skip(10).skip(10).skip(10))
        == """(
    stream([0])
    .skip(until=10)
    .skip(until=10)
    .skip(until=10)
    .skip(until=10)
    .skip(until=10)
)"""
    )


def test_repr_buffer() -> None:
    assert repr(stream([0]).buffer(3)) == "stream([0]).buffer(3)"


def test_repr_catch() -> None:
    assert (
        repr(stream([0]).catch(TypeError, replace=async_identity))
        == """(
    stream([0])
    .catch(TypeError, where=None, do=None, replace=async_identity, stop=False)
)"""
    )

    assert (
        repr(stream([0]).catch((TypeError, ValueError, ZeroDivisionError)))
        == """(
    stream([0])
    .catch((TypeError, ValueError, ZeroDivisionError), where=None, do=None, replace=None, stop=False)
)"""
    )


def test_repr_do() -> None:
    s = stream([0]).do(lambda _: _)
    assert repr(s) == "stream([0]).do(<lambda>, concurrency=1, as_completed=False)"


def test_repr_filter() -> None:
    s = stream([0]).filter(bool)
    assert repr(s) == "stream([0]).filter(bool)"


def test_repr_flatten() -> None:
    s = stream(["foo"]).flatten(concurrency=4)
    assert repr(s) == "stream(['foo']).flatten(concurrency=4)"


def test_repr_group() -> None:
    s = stream([0]).group(100, by=async_identity)
    assert repr(s) == "stream([0]).group(up_to=100, every=None, by=async_identity)"


def test_repr_map() -> None:
    s = stream([0]).map(async_identity, as_completed=True)
    assert (
        repr(s) == "stream([0]).map(async_identity, concurrency=1, as_completed=True)"
    )


def test_repr_observe() -> None:
    s_without_do = stream([0]).observe("ints", every=10)
    assert repr(s_without_do) == "stream([0]).observe('ints', every=10)"

    s_with_do = stream([0]).observe("ints", every=10, do=print)
    assert repr(s_with_do) == "stream([0]).observe('ints', every=10, do=print)"


def test_repr_skip() -> None:
    s = stream([0]).skip(until=async_identity)
    assert repr(s) == "stream([0]).skip(until=async_identity)"


def test_repr_take() -> None:
    s = stream([0]).take(until=async_identity)
    assert repr(s) == "stream([0]).take(until=async_identity)"


def test_repr_throttle() -> None:
    s = stream([0]).throttle(64, per=datetime.timedelta(seconds=1))
    assert repr(s) == "stream([0]).throttle(64, per=datetime.timedelta(seconds=1))"


def test_repr_star() -> None:
    s = stream([(0, 1)]).filter(star(print))
    assert repr(s) == "stream([(0, 1)]).filter(star(print))"

    s2 = stream([(0, 1)]).filter(star(async_identity))
    assert repr(s2) == "stream([(0, 1)]).filter(star(async_identity))"


def test_repr_cast() -> None:
    s = stream([0])
    assert repr(s.cast(str)) == repr(s)


def test_repr_with_local_fn() -> None:
    def foo(_: Any) -> None: ...
    async def afoo(_: Any) -> None: ...

    s = stream([0]).filter(foo).filter(afoo)
    assert repr(s) == "stream([0]).filter(foo).filter(afoo)"
