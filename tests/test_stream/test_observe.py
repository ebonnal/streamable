import datetime
from typing import Any, Callable, Iterable, List, NamedTuple, Union

import pytest

from streamable import stream
from streamable._tools._func import asyncify
from streamable._tools._logging import logfmt_str_escape
from tests.utils.functions import identity, slow_identity, slow_identity_duration
from tests.utils.iteration import ITERABLE_TYPES, IterableType, alist_or_list


class Log(NamedTuple):
    """Represents a log entry from observe."""

    errors: int
    yields: int

    @staticmethod
    def from_logfmt(logfmt_str: str) -> "Log":
        """Parses a logfmt string into a Log instance."""
        parts = dict(part.split("=", 1) for part in logfmt_str.split())
        return Log(
            errors=int(parts["errors"]),
            yields=int(parts["elements"]),
        )


def inverse(
    chars: Iterable[str],
    logs: List[Log] = [],
    every: Union[None, int, datetime.timedelta] = None,
) -> stream[float]:
    return (
        stream(chars)
        .map(int)
        .map(lambda n: 1 / n)
        .observe(
            "inverses",
            every=every,
            do=lambda obs: logs.append(Log.from_logfmt(str(obs))),
        )
        .catch(ValueError)
    )


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_yields_upstream_elements(itype: IterableType) -> None:
    """`observe` should yield upstream elements"""
    assert alist_or_list(inverse("12---3456----7"), itype=itype) == list(
        map(lambda n: 1 / n, range(1, 8))
    )


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_empty_stream(itype: IterableType) -> None:
    """Observe Empty Stream."""
    logs: List[Log] = []

    assert alist_or_list(inverse("", logs), itype=itype) == []
    assert logs == [Log(errors=0, yields=0)]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_none_reraises(itype: IterableType) -> None:
    """Observe Every None Reraises."""
    logs: List[Log] = []

    with pytest.raises(ZeroDivisionError):
        alist_or_list(inverse("12---3456----07", logs), itype=itype)
    assert logs == [
        Log(errors=0, yields=1),
        Log(errors=0, yields=2),
        Log(errors=1, yields=2),
        Log(errors=2, yields=2),
        Log(errors=3, yields=4),
        Log(errors=4, yields=6),
        Log(errors=8, yields=6),
    ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_none_with_catch(itype: IterableType) -> None:
    """Observe Every None With Catch."""
    logs: List[Log] = []

    alist_or_list(
        inverse("12---3456----07", logs).catch(ZeroDivisionError), itype=itype
    )
    assert logs == [
        Log(errors=0, yields=1),
        Log(errors=0, yields=2),
        Log(errors=1, yields=2),
        Log(errors=2, yields=2),
        Log(errors=3, yields=4),
        Log(errors=4, yields=6),
        Log(errors=8, yields=6),
        Log(errors=8, yields=7),
    ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_none_skips_redundant(itype: IterableType) -> None:
    """Observe Every None Skips Redundant."""
    logs: List[Log] = []
    alist_or_list(inverse("12---3456----0", logs).catch(ZeroDivisionError), itype=itype)
    assert logs == [
        Log(errors=0, yields=1),
        Log(errors=0, yields=2),
        Log(errors=1, yields=2),
        Log(errors=2, yields=2),
        Log(errors=3, yields=4),
        Log(errors=4, yields=6),
        Log(errors=8, yields=6),
    ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_2_reraises(itype: IterableType) -> None:
    """Observe Every 2 Reraises."""
    logs: List[Log] = []
    with pytest.raises(ZeroDivisionError):
        alist_or_list(inverse("12---3456----07", logs, every=2), itype=itype)
    assert logs == [
        Log(errors=0, yields=1),
        Log(errors=0, yields=2),
        Log(errors=1, yields=2),
        Log(errors=2, yields=2),
        Log(errors=3, yields=4),
        Log(errors=3, yields=6),
        Log(errors=4, yields=6),
        Log(errors=6, yields=6),
        Log(errors=8, yields=6),
    ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_2_with_catch(itype: IterableType) -> None:
    """Observe Every 2 With Catch."""
    logs: List[Log] = []
    alist_or_list(
        inverse("12---3456----07", logs, every=2).catch(ZeroDivisionError), itype=itype
    )
    assert logs == [
        Log(errors=0, yields=1),
        Log(errors=0, yields=2),
        Log(errors=1, yields=2),
        Log(errors=2, yields=2),
        Log(errors=3, yields=4),
        Log(errors=3, yields=6),
        Log(errors=4, yields=6),
        Log(errors=6, yields=6),
        Log(errors=8, yields=6),
        Log(errors=8, yields=7),
    ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_2_skips_redundant(itype: IterableType) -> None:
    """Observe Every 2 Skips Redundant."""
    logs: List[Log] = []
    alist_or_list(
        inverse("12---3456----0", logs, every=2).catch(ZeroDivisionError), itype=itype
    )
    assert logs == [
        Log(errors=0, yields=1),
        Log(errors=0, yields=2),
        Log(errors=1, yields=2),
        Log(errors=2, yields=2),
        Log(errors=3, yields=4),
        Log(errors=3, yields=6),
        Log(errors=4, yields=6),
        Log(errors=6, yields=6),
        Log(errors=8, yields=6),
    ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_timedelta_reraises(itype: IterableType) -> None:
    """Observe Every Timedelta Reraises."""
    logs: List[Log] = []
    with pytest.raises(ZeroDivisionError):
        alist_or_list(
            inverse("12---3456----07", logs, every=datetime.timedelta(seconds=1)),
            itype=itype,
        )
    assert logs == [Log(errors=0, yields=1)]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_timedelta_with_catch(itype: IterableType) -> None:
    """Observe Every Timedelta With Catch."""
    logs: List[Log] = []
    alist_or_list(
        inverse("12---3456----07", logs, every=datetime.timedelta(seconds=1)).catch(
            ZeroDivisionError
        ),
        itype=itype,
    )
    assert logs == [
        Log(errors=0, yields=1),
        Log(errors=8, yields=7),
    ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_timedelta_skips_redundant(itype: IterableType) -> None:
    """Observe Every Timedelta Skips Redundant."""
    logs: List[Log] = []
    alist_or_list(
        inverse("12---3456----0", logs, every=datetime.timedelta(seconds=1)).catch(
            ZeroDivisionError
        ),
        itype=itype,
    )
    assert logs == [
        Log(errors=0, yields=1),
        Log(errors=8, yields=6),
    ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_timedelta_frequent(itype: IterableType) -> None:
    """Observe with `every` slightly under slow_identity_duration should emit one log per yield/error."""

    logs: List[Log] = []

    digits = "12---3456----0"
    alist_or_list(
        inverse(
            map(slow_identity, digits),
            logs,
            every=datetime.timedelta(seconds=0.9 * slow_identity_duration),
        ).catch(ZeroDivisionError),
        itype=itype,
    )
    assert len(digits) == len(logs)


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_do(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Observe Do."""
    observations: List[stream.Observation] = []
    ints = list(range(8))
    assert (
        alist_or_list(
            stream(ints).observe("ints", every=2, do=adapt(observations.append)),
            itype,
        )
        == ints
    )
    assert [observation.elements for observation in observations] == [1, 2, 4, 6, 8]


def test_escape():
    """Test logfmt string escaping."""
    assert logfmt_str_escape("ints") == "ints"
    assert logfmt_str_escape("in ts") == '"in ts"'
    assert logfmt_str_escape("in\\ts") == r'"in\\ts"'
    assert logfmt_str_escape('"ints"') == r'"\"ints\""'
