import datetime
from typing import Any, Callable, Iterable, List, NamedTuple, Union

import pytest

from streamable import stream
from streamable._tools._func import asyncify
from tests.utils.functions import identity
from tests.utils.iteration import (
    ITERABLE_TYPES,
    IterableType,
    aiterate_or_iterate,
    alist_or_list,
)


class Counts(NamedTuple):
    """Represents a log entry from observe."""

    errors: int
    elements: int

    @staticmethod
    def from_observation(observation: stream.Observation) -> "Counts":
        return Counts(
            errors=observation.errors,
            elements=observation.elements,
        )


THROTTLE_PER = datetime.timedelta(seconds=0.1)


def inverse(
    chars: Iterable[str],
    logs: List[Counts] = [],
    every: Union[None, int, datetime.timedelta] = None,
) -> stream[float]:
    return (
        stream(chars)
        .map(int)
        .map(lambda n: 1 / n)
        .throttle(1, per=THROTTLE_PER)
        .observe(
            "inverses",
            every=every,
            do=lambda obs: logs.append(Counts.from_observation(obs)),
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
    logs: List[Counts] = []

    assert alist_or_list(inverse("", logs), itype=itype) == []
    assert logs == [Counts(errors=0, elements=0)]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_none_reraises(itype: IterableType) -> None:
    """Observe Every None Reraises."""
    logs: List[Counts] = []

    with pytest.raises(ZeroDivisionError):
        alist_or_list(inverse("12---3456----07", logs), itype=itype)
    assert logs == [
        Counts(errors=0, elements=1),
        Counts(errors=0, elements=2),
        Counts(errors=1, elements=2),
        Counts(errors=2, elements=2),
        Counts(errors=3, elements=4),
        Counts(errors=4, elements=6),
        Counts(errors=8, elements=6),
    ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_none_with_catch(itype: IterableType) -> None:
    """Observe Every None With Catch."""
    logs: List[Counts] = []

    alist_or_list(
        inverse("12---3456----07", logs).catch(ZeroDivisionError), itype=itype
    )
    assert logs == [
        Counts(errors=0, elements=1),
        Counts(errors=0, elements=2),
        Counts(errors=1, elements=2),
        Counts(errors=2, elements=2),
        Counts(errors=3, elements=4),
        Counts(errors=4, elements=6),
        Counts(errors=8, elements=6),
        Counts(errors=8, elements=7),
    ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_none_skips_redundant(itype: IterableType) -> None:
    """Observe Every None Skips Redundant."""
    logs: List[Counts] = []
    alist_or_list(inverse("12---3456----0", logs).catch(ZeroDivisionError), itype=itype)
    assert logs == [
        Counts(errors=0, elements=1),
        Counts(errors=0, elements=2),
        Counts(errors=1, elements=2),
        Counts(errors=2, elements=2),
        Counts(errors=3, elements=4),
        Counts(errors=4, elements=6),
        Counts(errors=8, elements=6),
    ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_1_reraises(itype: IterableType) -> None:
    """Observe Every 2 Reraises."""
    logs: List[Counts] = []
    with pytest.raises(ZeroDivisionError):
        alist_or_list(inverse("12---3456----07", logs, every=1), itype=itype)
    assert logs == [
        Counts(errors=0, elements=1),
        Counts(errors=0, elements=2),
        Counts(errors=1, elements=2),
        Counts(errors=2, elements=2),
        Counts(errors=3, elements=2),
        Counts(errors=3, elements=3),
        Counts(errors=3, elements=4),
        Counts(errors=3, elements=5),
        Counts(errors=3, elements=6),
        Counts(errors=4, elements=6),
        Counts(errors=5, elements=6),
        Counts(errors=6, elements=6),
        Counts(errors=7, elements=6),
        Counts(errors=8, elements=6),
    ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_2_reraises(itype: IterableType) -> None:
    """Observe Every 2 Reraises."""
    logs: List[Counts] = []
    with pytest.raises(ZeroDivisionError):
        alist_or_list(inverse("12---3456----07", logs, every=2), itype=itype)
    assert logs == [
        Counts(errors=0, elements=1),
        Counts(errors=0, elements=2),
        Counts(errors=1, elements=2),
        Counts(errors=2, elements=2),
        Counts(errors=3, elements=4),
        Counts(errors=3, elements=6),
        Counts(errors=4, elements=6),
        Counts(errors=6, elements=6),
        Counts(errors=8, elements=6),
    ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_2_with_catch(itype: IterableType) -> None:
    """Observe Every 2 With Catch."""
    logs: List[Counts] = []
    alist_or_list(
        inverse("12---3456----07", logs, every=2).catch(ZeroDivisionError), itype=itype
    )
    assert logs == [
        Counts(errors=0, elements=1),
        Counts(errors=0, elements=2),
        Counts(errors=1, elements=2),
        Counts(errors=2, elements=2),
        Counts(errors=3, elements=4),
        Counts(errors=3, elements=6),
        Counts(errors=4, elements=6),
        Counts(errors=6, elements=6),
        Counts(errors=8, elements=6),
        Counts(errors=8, elements=7),
    ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_2_skips_redundant(itype: IterableType) -> None:
    """Observe Every 2 Skips Redundant."""
    logs: List[Counts] = []
    alist_or_list(
        inverse("12---3456----0", logs, every=2).catch(ZeroDivisionError), itype=itype
    )
    assert logs == [
        Counts(errors=0, elements=1),
        Counts(errors=0, elements=2),
        Counts(errors=1, elements=2),
        Counts(errors=2, elements=2),
        Counts(errors=3, elements=4),
        Counts(errors=3, elements=6),
        Counts(errors=4, elements=6),
        Counts(errors=6, elements=6),
        Counts(errors=8, elements=6),
    ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_timedelta_reraises(itype: IterableType) -> None:
    """Observe Every Timedelta Reraises."""
    logs: List[Counts] = []
    with pytest.raises(ZeroDivisionError):
        aiterate_or_iterate(
            inverse("12---3456----07", logs, every=datetime.timedelta(days=1)),
            itype=itype,
        )
    assert logs == [Counts(errors=0, elements=0)]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_timedelta_with_catch(itype: IterableType) -> None:
    """Observe Every Timedelta With Catch."""
    logs: List[Counts] = []
    alist_or_list(
        inverse("12---3456----07", logs, every=datetime.timedelta(days=1)).catch(
            ZeroDivisionError
        ),
        itype=itype,
    )
    assert logs == [
        Counts(errors=0, elements=0),
        Counts(errors=8, elements=7),
    ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_timedelta_skips_redundant(itype: IterableType) -> None:
    """Observe Every Timedelta Skips Redundant."""
    logs: List[Counts] = []
    alist_or_list(
        inverse("12---3456----0", logs, every=datetime.timedelta(days=1)).catch(
            ZeroDivisionError
        ),
        itype=itype,
    )
    assert logs == [
        Counts(errors=0, elements=0),
        Counts(errors=8, elements=6),
    ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_timedelta_frequent(itype: IterableType) -> None:
    """Observe with `every` slightly under slow_identity_duration should emit one log per yield/error."""

    logs: List[Counts] = []

    alist_or_list(
        inverse(
            "12---3456----0",
            logs,
            every=datetime.timedelta(seconds=0.95 * THROTTLE_PER.total_seconds()),
        ).catch(ZeroDivisionError),
        itype=itype,
    )
    assert logs == [
        Counts(errors=0, elements=0),
        Counts(errors=0, elements=1),
        Counts(errors=0, elements=2),
        Counts(errors=1, elements=2),
        Counts(errors=2, elements=2),
        Counts(errors=3, elements=2),
        Counts(errors=3, elements=3),
        Counts(errors=3, elements=4),
        Counts(errors=3, elements=5),
        Counts(errors=3, elements=6),
        Counts(errors=4, elements=6),
        Counts(errors=5, elements=6),
        Counts(errors=6, elements=6),
        Counts(errors=7, elements=6),
        Counts(errors=8, elements=6),
    ]


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
