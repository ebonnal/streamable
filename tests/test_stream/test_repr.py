import datetime
from typing import Any, List


from streamable._stream import stream
from streamable._tools._func import star
from tests.utils.functions import async_identity
from tests.utils.source import INTEGERS


def test_repr_empty_stream() -> None:
    """Repr Empty Stream."""
    assert str(stream(INTEGERS)) == "stream(range(0, 256))"


def test_repr_one_operation() -> None:
    """Repr One Operation."""
    assert str(stream(INTEGERS).skip(10)) == "stream(range(0, 256)).skip(until=10)"


def test_repr_two_operations() -> None:
    """Repr Two Operations."""
    assert (
        str(stream(INTEGERS).skip(10).skip(10))
        == "stream(range(0, 256)).skip(until=10).skip(until=10)"
    )


def test_repr_multiline_formatting() -> None:
    """Repr Multiline Formatting."""
    assert (
        str(stream(INTEGERS).skip(10).skip(10).skip(10).skip(10))
        == """(
    stream(range(0, 256))
    .skip(until=10)
    .skip(until=10)
    .skip(until=10)
    .skip(until=10)
)"""
    )


def test_repr_buffer() -> None:
    """Repr Buffer."""
    assert str(stream(INTEGERS).buffer(3)) == "stream(range(0, 256)).buffer(3)"


def test_repr_catch() -> None:
    """Repr Catch."""
    assert (
        str(stream(INTEGERS).catch(TypeError, replace=async_identity))
        == """(
    stream(range(0, 256))
    .catch(TypeError, where=None, do=None, replace=async_identity, stop=False)
)"""
    )

    assert (
        str(stream(INTEGERS).catch((TypeError, ValueError, ZeroDivisionError)))
        == """(
    stream(range(0, 256))
    .catch((TypeError, ValueError, ZeroDivisionError), where=None, do=None, replace=None, stop=False)
)"""
    )


def test_repr_do() -> None:
    """Repr Do."""
    s = stream(INTEGERS).do(lambda _: _)
    assert (
        str(s)
        == "stream(range(0, 256)).do(<lambda>, concurrency=1, as_completed=False)"
    )


def test_repr_filter() -> None:
    """Repr Filter."""
    s = stream(INTEGERS).filter(bool)
    assert str(s) == "stream(range(0, 256)).filter(bool)"


def test_repr_flatten() -> None:
    """Repr Flatten."""
    s = stream([INTEGERS]).flatten(concurrency=4)
    assert str(s) == "stream([range(0, 256)]).flatten(concurrency=4)"


def test_repr_group() -> None:
    """Repr Group."""
    s = stream(INTEGERS).group(100, by=async_identity)
    assert (
        str(s)
        == "stream(range(0, 256)).group(up_to=100, every=None, by=async_identity)"
    )


def test_repr_map() -> None:
    """Repr Map."""
    s = stream(INTEGERS).map(async_identity, as_completed=True)
    assert (
        str(s)
        == "stream(range(0, 256)).map(async_identity, concurrency=1, as_completed=True)"
    )


def test_repr_observe() -> None:
    """Repr Observe."""
    s = stream(INTEGERS).observe("foos", every=10, do=print)
    assert str(s) == "stream(range(0, 256)).observe('foos', every=10, do=print)"


def test_repr_skip() -> None:
    """Repr Skip."""
    s = stream(INTEGERS).skip(until=async_identity)
    assert str(s) == "stream(range(0, 256)).skip(until=async_identity)"


def test_repr_take() -> None:
    """Repr Take."""
    s = stream(INTEGERS).take(until=async_identity)
    assert str(s) == "stream(range(0, 256)).take(until=async_identity)"


def test_repr_throttle() -> None:
    """Repr Throttle."""
    s = stream(INTEGERS).throttle(64, per=datetime.timedelta(seconds=1))
    assert (
        str(s)
        == "stream(range(0, 256)).throttle(64, per=datetime.timedelta(seconds=1))"
    )


def test_repr_star() -> None:
    """Repr Star."""
    s = stream([(0, 1)]).filter(star(print))
    assert repr(s) == "stream([(0, 1)]).filter(star(<built-in function print>))"
    assert str(s) == "stream([(0, 1)]).filter(star(print))"
    s2 = stream([(0, 1)]).filter(star(async_identity))
    assert (
        repr(s2)
        == f"stream([(0, 1)]).filter(star(<function async_identity at 0x{id(async_identity):x}>))"
    )
    assert str(s2) == "stream([(0, 1)]).filter(star(async_identity))"


def test_repr_cast() -> None:
    """Repr Cast."""
    assert str(stream([]).cast(List[int])) == "stream([])"


def test_repr_and_str_local_fn() -> None:
    """Repr And Str Local Fn."""

    async def foo(_: Any) -> None:
        return None  # pragma: no cover

    s = stream(INTEGERS).filter(foo)
    assert (
        repr(s)
        == f"""(
    stream(range(0, 256))
    .filter(<function test_repr_and_str_local_fn.<locals>.foo at 0x{id(foo):x}>)
)"""
    )
    assert str(s) == "stream(range(0, 256)).filter(foo)"
