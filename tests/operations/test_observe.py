"""
Tests for observe operation.

Observe logs iteration progress including errors and emissions at specified intervals.
"""

import datetime
from typing import Any, Callable, Iterable, List, NamedTuple, Union
from unittest.mock import patch

import pytest

from streamable import stream
from streamable._tools._func import asyncify
from streamable._tools._logging import logfmt_str_escape
from tests.utils import (
    ITERABLE_TYPES,
    IterableType,
    identity,
    slow_identity,
    slow_identity_duration,
    to_list,
)


# ============================================================================
# Helper Classes
# ============================================================================


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
            yields=int(parts["emissions"]),
        )


# ============================================================================
# Basic Functionality Tests
# ============================================================================


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_yields_upstream_elements(itype: IterableType) -> None:
    """Observe should yield upstream elements."""

    def inverse(
        chars: Iterable[str], every: Union[None, int, datetime.timedelta] = None
    ) -> stream[float]:
        return (
            stream(chars)
            .map(int)
            .map(lambda n: 1 / n)
            .observe("inverses", every=every)
            .catch(ValueError)
        )

    # `observe` should yield upstream elements
    assert to_list(inverse("12---3456----7"), itype=itype) == list(
        map(lambda n: 1 / n, range(1, 8))
    )


# ============================================================================
# Logging Tests - every == None
# ============================================================================


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_logging_every_none_reraises(itype: IterableType) -> None:
    """Observe should reraise exceptions when every is None."""

    def inverse(
        chars: Iterable[str], every: Union[None, int, datetime.timedelta] = None
    ) -> stream[float]:
        return (
            stream(chars)
            .map(int)
            .map(lambda n: 1 / n)
            .observe("inverses", every=every)
            .catch(ValueError)
        )

    logs: List[Log] = []
    with patch(
        "logging.Logger.info",
        lambda self, msg: logs.append(Log.from_logfmt(msg)),
    ):
        # `observe` should reraise
        with pytest.raises(ZeroDivisionError):
            to_list(inverse("12---3456----07"), itype=itype)
        # `observe` errors and yields independently
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
def test_observe_logging_every_none_with_catch(itype: IterableType) -> None:
    """Observe should produce one last log on StopIteration when exceptions are caught."""

    def inverse(
        chars: Iterable[str], every: Union[None, int, datetime.timedelta] = None
    ) -> stream[float]:
        return (
            stream(chars)
            .map(int)
            .map(lambda n: 1 / n)
            .observe("inverses", every=every)
            .catch(ValueError)
        )

    logs: List[Log] = []
    with patch(
        "logging.Logger.info",
        lambda self, msg: logs.append(Log.from_logfmt(msg)),
    ):
        logs.clear()
        to_list(inverse("12---3456----07").catch(ZeroDivisionError), itype=itype)
        # `observe` should produce one last log on StopIteration
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
def test_observe_logging_every_none_skips_redundant(itype: IterableType) -> None:
    """Observe should skip redundant last log on StopIteration."""

    def inverse(
        chars: Iterable[str], every: Union[None, int, datetime.timedelta] = None
    ) -> stream[float]:
        return (
            stream(chars)
            .map(int)
            .map(lambda n: 1 / n)
            .observe("inverses", every=every)
            .catch(ValueError)
        )

    logs: List[Log] = []
    with patch(
        "logging.Logger.info",
        lambda self, msg: logs.append(Log.from_logfmt(msg)),
    ):
        logs.clear()
        to_list(inverse("12---3456----0").catch(ZeroDivisionError), itype=itype)
        # `observe` should skip redundant last log on StopIteration
        assert logs == [
            Log(errors=0, yields=1),
            Log(errors=0, yields=2),
            Log(errors=1, yields=2),
            Log(errors=2, yields=2),
            Log(errors=3, yields=4),
            Log(errors=4, yields=6),
            Log(errors=8, yields=6),
        ]


# ============================================================================
# Logging Tests - every == 2
# ============================================================================


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_logging_every_2_reraises(itype: IterableType) -> None:
    """Observe should reraise exceptions when every is 2."""

    def inverse(
        chars: Iterable[str], every: Union[None, int, datetime.timedelta] = None
    ) -> stream[float]:
        return (
            stream(chars)
            .map(int)
            .map(lambda n: 1 / n)
            .observe("inverses", every=every)
            .catch(ValueError)
        )

    logs: List[Log] = []
    with patch(
        "logging.Logger.info",
        lambda self, msg: logs.append(Log.from_logfmt(msg)),
    ):
        # `observe` should reraise
        logs.clear()
        with pytest.raises(ZeroDivisionError):
            to_list(inverse("12---3456----07", every=2), itype=itype)
        # `observe` errors and yields independently
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
def test_observe_logging_every_2_with_catch(itype: IterableType) -> None:
    """Observe should produce one last log on StopIteration when every is 2 and exceptions are caught."""

    def inverse(
        chars: Iterable[str], every: Union[None, int, datetime.timedelta] = None
    ) -> stream[float]:
        return (
            stream(chars)
            .map(int)
            .map(lambda n: 1 / n)
            .observe("inverses", every=every)
            .catch(ValueError)
        )

    logs: List[Log] = []
    with patch(
        "logging.Logger.info",
        lambda self, msg: logs.append(Log.from_logfmt(msg)),
    ):
        logs.clear()
        to_list(
            inverse("12---3456----07", every=2).catch(ZeroDivisionError), itype=itype
        )
        # `observe` should produce one last log on StopIteration
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
def test_observe_logging_every_2_skips_redundant(itype: IterableType) -> None:
    """Observe should skip redundant last log when every is 2."""

    def inverse(
        chars: Iterable[str], every: Union[None, int, datetime.timedelta] = None
    ) -> stream[float]:
        return (
            stream(chars)
            .map(int)
            .map(lambda n: 1 / n)
            .observe("inverses", every=every)
            .catch(ValueError)
        )

    logs: List[Log] = []
    with patch(
        "logging.Logger.info",
        lambda self, msg: logs.append(Log.from_logfmt(msg)),
    ):
        logs.clear()
        to_list(
            inverse("12---3456----0", every=2).catch(ZeroDivisionError), itype=itype
        )
        # `observe` should skip redundant last log on StopIteration
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


# ============================================================================
# Logging Tests - every == timedelta
# ============================================================================


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_logging_every_timedelta_reraises(itype: IterableType) -> None:
    """Observe should reraise exceptions when every is a timedelta."""

    def inverse(
        chars: Iterable[str], every: Union[None, int, datetime.timedelta] = None
    ) -> stream[float]:
        return (
            stream(chars)
            .map(int)
            .map(lambda n: 1 / n)
            .observe("inverses", every=every)
            .catch(ValueError)
        )

    logs: List[Log] = []
    with patch(
        "logging.Logger.info",
        lambda self, msg: logs.append(Log.from_logfmt(msg)),
    ):
        # `observe` should reraise
        logs.clear()
        with pytest.raises(ZeroDivisionError):
            to_list(
                inverse("12---3456----07", every=datetime.timedelta(seconds=1)),
                itype=itype,
            )
        # `observe` errors and yields independently
        assert logs == [Log(errors=0, yields=1)]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_logging_every_timedelta_with_catch(itype: IterableType) -> None:
    """Observe should produce one last log on StopIteration when every is a timedelta and exceptions are caught."""

    def inverse(
        chars: Iterable[str], every: Union[None, int, datetime.timedelta] = None
    ) -> stream[float]:
        return (
            stream(chars)
            .map(int)
            .map(lambda n: 1 / n)
            .observe("inverses", every=every)
            .catch(ValueError)
        )

    logs: List[Log] = []
    with patch(
        "logging.Logger.info",
        lambda self, msg: logs.append(Log.from_logfmt(msg)),
    ):
        logs.clear()
        to_list(
            inverse("12---3456----07", every=datetime.timedelta(seconds=1)).catch(
                ZeroDivisionError
            ),
            itype=itype,
        )
        # `observe` should produce one last log on StopIteration
        assert logs == [
            Log(errors=0, yields=1),
            Log(errors=8, yields=7),
        ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_logging_every_timedelta_skips_redundant(itype: IterableType) -> None:
    """Observe should skip redundant last log when every is a timedelta."""

    def inverse(
        chars: Iterable[str], every: Union[None, int, datetime.timedelta] = None
    ) -> stream[float]:
        return (
            stream(chars)
            .map(int)
            .map(lambda n: 1 / n)
            .observe("inverses", every=every)
            .catch(ValueError)
        )

    logs: List[Log] = []
    with patch(
        "logging.Logger.info",
        lambda self, msg: logs.append(Log.from_logfmt(msg)),
    ):
        logs.clear()
        to_list(
            inverse("12---3456----0", every=datetime.timedelta(seconds=1)).catch(
                ZeroDivisionError
            ),
            itype=itype,
        )
        # `observe` should skip redundant last log on StopIteration
        assert logs == [
            Log(errors=0, yields=1),
            Log(errors=8, yields=6),
        ]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_logging_every_timedelta_frequent(itype: IterableType) -> None:
    """Observe with `every` slightly under slow_identity_duration should emit one log per yield/error."""

    def inverse(
        chars: Iterable[str], every: Union[None, int, datetime.timedelta] = None
    ) -> stream[float]:
        return (
            stream(chars)
            .map(int)
            .map(lambda n: 1 / n)
            .observe("inverses", every=every)
            .catch(ValueError)
        )

    logs: List[Log] = []
    with patch(
        "logging.Logger.info",
        lambda self, msg: logs.append(Log.from_logfmt(msg)),
    ):
        logs.clear()
        digits = "12---3456----0"
        to_list(
            inverse(
                map(slow_identity, digits),
                every=datetime.timedelta(seconds=0.9 * slow_identity_duration),
            ).catch(ZeroDivisionError),
            itype=itype,
        )
        # `observe` with `every` slightly under slow_identity_duration should emit one log per yield/error
        assert len(digits) == len(logs)


# ============================================================================
# Custom How Function Tests
# ============================================================================


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe_how(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Observe should call the how function with log messages."""
    observed: List[str] = []
    ints = list(range(8))
    assert (
        to_list(
            stream(ints).observe(
                "ints", every=2, how=adapt(lambda msg: observed.append(msg[-20:]))
            ),
            itype,
        )
        == ints
    )
    assert observed == [
        "errors=0 emissions=1",
        "errors=0 emissions=2",
        "errors=0 emissions=4",
        "errors=0 emissions=6",
        "errors=0 emissions=8",
    ]


# ============================================================================
# Utility Tests
# ============================================================================


def test_escape():
    """Test logfmt string escaping."""
    assert logfmt_str_escape("ints") == '"ints"'
    assert logfmt_str_escape("in ts") == '"in ts"'
    assert logfmt_str_escape("in\\ts") == r'"in\\ts"'
    assert logfmt_str_escape('"ints"') == r'"\"ints\""'
