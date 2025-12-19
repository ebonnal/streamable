import datetime
from typing import (
    Iterable,
    List,
    NamedTuple,
    Optional,
    Union,
)
from unittest.mock import patch

import pytest

from streamable import stream
from streamable._utils._logging import SubjectEscapingFormatter
from tests.utils import (
    ITERABLE_TYPES,
    IterableType,
    slow_identity,
    slow_identity_duration,
    to_list,
)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_observe(itype: IterableType) -> None:
    def inverse(
        chars: Iterable[str], every: Optional[Union[int, datetime.timedelta]] = None
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

    class Log(NamedTuple):
        errors: int
        yields: int

    logs: List[Log] = []
    with patch(
        "logging.Logger.info",
        lambda self, msg, extra: logs.append(Log(extra["errors"], extra["emissions"])),
    ):
        #################
        # every == None #
        #################

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

        ##############
        # every == 2 #
        ##############

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

        ###############
        # every == 1s #
        ###############

        # `observe` should reraise
        logs.clear()
        with pytest.raises(ZeroDivisionError):
            to_list(
                inverse("12---3456----07", every=datetime.timedelta(seconds=1)),
                itype=itype,
            )
        # `observe` errors and yields independently
        assert logs == [Log(errors=0, yields=1)]

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


def test_escape():
    assert SubjectEscapingFormatter._escape("ints") == '"ints"'
    assert SubjectEscapingFormatter._escape("in ts") == '"in ts"'
    assert SubjectEscapingFormatter._escape("in\\ts") == r'"in\\ts"'
    assert SubjectEscapingFormatter._escape('"ints"') == r'"\"ints\""'
