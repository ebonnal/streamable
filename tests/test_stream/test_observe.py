import datetime
from typing import Any, Callable, Iterable, List, NamedTuple, Union

import pytest

from streamable import stream
from streamable._tools._func import asyncify
from tests.utils.error import TestError
from tests.utils.func import identity, inverse, throw_func
from tests.utils.iter import (
    ITERABLE_TYPES,
    IterableType,
    aiter_or_iter,
    alist_or_list,
    anext_or_next,
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


def to_inverses(
    chars: Iterable[str],
    logs: List[Counts] = [],
    every: Union[None, int, datetime.timedelta] = None,
) -> stream[float]:
    """
    Convert characters to integers, inverse, throttle and observe them.
    The potential `ValueError`s (`int("-")`) are observed and caught.
    The potential `ZeroDivisionError`s (`inverse(0)`) are observed but not caught.
    """

    def do(obs: stream.Observation) -> None:
        logs.append(Counts.from_observation(obs))

    return (
        stream(chars)
        .map(int)
        .map(inverse)
        .throttle(1, per=THROTTLE_PER)
        .observe("inverses", every=every, do=do)
        .catch(ValueError)
    )


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_yields_upstream_elements(itype: IterableType) -> None:
    s = to_inverses("12---3456----7")
    assert alist_or_list(s, itype) == [1 / i for i in range(1, 8)]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_empty_stream(itype: IterableType) -> None:
    logs: List[Counts] = []

    assert alist_or_list(to_inverses("", logs), itype) == []
    assert logs == [Counts(errors=0, elements=0)]


@pytest.mark.parametrize("every", [None, 2, datetime.timedelta(days=1)])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_reraises(
    itype: IterableType, every: Union[None, int, datetime.timedelta]
) -> None:
    it = aiter_or_iter(to_inverses("10", every=every), itype)
    assert anext_or_next(it, itype) == 1
    with pytest.raises(ZeroDivisionError):
        anext_or_next(it, itype)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_none_with_catch(itype: IterableType) -> None:
    """Observe when error or element counts reach a power of 2"""
    logs: List[Counts] = []
    s = to_inverses("12---3456----07", logs).catch(ZeroDivisionError)
    alist_or_list(s, itype)
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
def test_observe_every_none_skips_last_observation_if_redundant(
    itype: IterableType,
) -> None:
    logs: List[Counts] = []
    alist_or_list(to_inverses("12---3456-", logs), itype)
    assert logs == [
        Counts(errors=0, elements=1),
        Counts(errors=0, elements=2),
        Counts(errors=1, elements=2),
        Counts(errors=2, elements=2),
        Counts(errors=3, elements=4),
        Counts(errors=4, elements=6),
    ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_1(itype: IterableType) -> None:
    logs: List[Counts] = []
    alist_or_list(to_inverses("12---3456-----", logs, every=1), itype)
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
def test_observe_every_2(itype: IterableType) -> None:
    logs: List[Counts] = []
    alist_or_list(to_inverses("12---3456-----", logs, every=2), itype)
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
def test_observe_every_2_skips_last_observation_if_redundant(
    itype: IterableType,
) -> None:
    logs: List[Counts] = []
    alist_or_list(to_inverses("12---3456-", logs, every=2), itype)
    assert logs == [
        Counts(errors=0, elements=1),
        Counts(errors=0, elements=2),
        Counts(errors=1, elements=2),
        Counts(errors=2, elements=2),
        Counts(errors=3, elements=4),
        Counts(errors=3, elements=6),
        Counts(errors=4, elements=6),
    ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_timedelta_adds_last_observation(itype: IterableType) -> None:
    logs: List[Counts] = []
    s = to_inverses("12---3456-----7", logs, every=datetime.timedelta(days=1))
    alist_or_list(s, itype)
    assert logs == [
        Counts(errors=0, elements=0),
        Counts(errors=8, elements=7),
    ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_every_timedelta_frequent(itype: IterableType) -> None:
    logs: List[Counts] = []
    # slightly under slow_identity_duration
    frequent_every = datetime.timedelta(seconds=0.95 * THROTTLE_PER.total_seconds())
    s = to_inverses("12---3456-----", logs, every=frequent_every)
    alist_or_list(s, itype)
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
    observations: List[stream.Observation] = []
    s = stream(range(8)).observe("ints", every=2, do=adapt(observations.append))
    alist_or_list(s, itype)
    assert [observation.elements for observation in observations] == [1, 2, 4, 6, 8]
