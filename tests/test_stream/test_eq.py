import datetime

from streamable import stream
from tests.utils.source import ints


def test_buffer_eq() -> None:
    s = ints.buffer(up_to=5)
    assert s == ints.buffer(up_to=5)
    assert s != ints.buffer(up_to=10)


def test_catch_eq() -> None:
    def where(_: Exception) -> bool:
        return True  # pragma: no cover

    def do(_: Exception) -> None:
        pass  # pragma: no cover

    def replace(_: Exception) -> int:
        return 1  # pragma: no cover

    s = ints.catch(errors=ValueError, where=where, do=do, replace=replace, stop=True)
    assert s == ints.catch(
        errors=ValueError, where=where, do=do, replace=replace, stop=True
    )
    assert s != ints.catch(
        errors=TypeError, where=where, do=do, replace=replace, stop=True
    )
    assert s != ints.catch(
        errors=ValueError, where=lambda e: False, do=do, replace=replace, stop=True
    )
    assert s != ints.catch(
        errors=ValueError, where=where, do=lambda e: 2, replace=replace, stop=True
    )
    assert s != ints.catch(
        errors=ValueError, where=where, do=do, replace=lambda e: 2, stop=True
    )
    assert s != ints.catch(
        errors=ValueError, where=where, do=do, replace=replace, stop=False
    )


def test_filter_eq() -> None:
    def where(_: int) -> bool:
        return True  # pragma: no cover

    s = ints.filter(where=where)
    assert s == ints.filter(where=where)
    assert s != ints.filter(where=lambda x: x < 0)


def test_flatten_eq() -> None:
    nested = stream([[1, 2], [3, 4]])

    s = nested.flatten(concurrency=2)
    assert s == nested.flatten(concurrency=2)
    assert s != nested.flatten(concurrency=3)


def test_do_eq() -> None:
    def effect(_: int) -> None:
        pass  # pragma: no cover

    s = ints.do(effect=effect, concurrency=2, as_completed=True)
    assert s == ints.do(effect=effect, concurrency=2, as_completed=True)
    assert s != ints.do(effect=lambda x: 1, concurrency=2, as_completed=True)
    assert s != ints.do(effect=effect, concurrency=3, as_completed=True)
    assert s != ints.do(effect=effect, concurrency=2, as_completed=False)


def test_group_eq() -> None:
    def by(_: int) -> int:
        return 1  # pragma: no cover

    s = ints.group(up_to=5, within=datetime.timedelta(seconds=1), by=by)
    assert s == ints.group(up_to=5, within=datetime.timedelta(seconds=1), by=by)
    assert s != ints.group(up_to=10, within=datetime.timedelta(seconds=1), by=by)
    assert s != ints.group(up_to=5, within=datetime.timedelta(seconds=2), by=by)
    assert s != ints.group(
        up_to=5, within=datetime.timedelta(seconds=1), by=lambda x: x % 3
    )


def test_map_eq() -> None:
    def into(_: int) -> int:
        return 1  # pragma: no cover

    s = ints.map(into=into, concurrency=2, as_completed=True)
    assert s == ints.map(into=into, concurrency=2, as_completed=True)
    assert s != ints.map(into=lambda x: x * 3, concurrency=2, as_completed=True)
    assert s != ints.map(into=into, concurrency=3, as_completed=True)
    assert s != ints.map(into=into, concurrency=2, as_completed=False)


def test_observe_eq() -> None:
    def do(obs: stream.Observation) -> None:
        pass  # pragma: no cover

    s = ints.observe(subject="test", every=10, do=do)
    assert s == ints.observe(subject="test", every=10, do=do)
    assert s != ints.observe(subject="other", every=10, do=do)
    assert s != ints.observe(subject="test", every=20, do=do)
    assert s != ints.observe(subject="test", every=10, do=lambda _: 1)


def test_skip_eq() -> None:
    s = ints.skip(until=5)
    assert s == ints.skip(until=5)
    assert s != ints.skip(until=10)


def test_take_eq() -> None:
    s = ints.take(until=5)
    assert s == ints.take(until=5)
    assert s != ints.take(until=10)


def test_throttle_eq() -> None:
    s = ints.throttle(up_to=5, per=datetime.timedelta(seconds=1))
    assert s == ints.throttle(up_to=5, per=datetime.timedelta(seconds=1))
    assert s != ints.throttle(up_to=10, per=datetime.timedelta(seconds=1))
    assert s != ints.throttle(up_to=5, per=datetime.timedelta(seconds=2))
