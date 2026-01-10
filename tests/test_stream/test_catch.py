from collections import Counter
from typing import Any, Callable, List, Type

import pytest

from streamable import stream
from streamable._tools._func import asyncify
from tests.utils.error import TestError
from tests.utils.functions import identity, throw, throw_for_odd_func
from tests.utils.iteration import (
    ITERABLE_TYPES,
    IterableType,
    alist_or_list,
    anext_or_next,
    aiter_or_iter,
    stopiteration_type,
)
from tests.utils.source import INTEGERS


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_yields_elements_without_exceptions(itype: IterableType) -> None:
    """Catch should yield elements in exception-less scenarios."""
    assert alist_or_list(stream(INTEGERS).catch(Exception), itype=itype) == list(
        INTEGERS
    )


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_ignores_matching_exceptions(itype: IterableType) -> None:
    """If the exception type matches, then the impacted element should be ignored."""

    def fn(i):
        return i / (3 - i)

    s = stream(INTEGERS).map(fn)
    safe_src = list(INTEGERS)
    del safe_src[3]
    assert alist_or_list(s.catch(ZeroDivisionError), itype=itype) == list(
        map(fn, safe_src)
    )


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_raises_non_matching_exceptions(itype: IterableType) -> None:
    def fn(i):
        return i / (3 - i)

    s = stream(INTEGERS).map(fn)
    # If a non-caught exception type occurs, then it should be raised.
    with pytest.raises(ZeroDivisionError):
        alist_or_list(s.catch(TestError), itype=itype)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_raises_first_non_caught_exception(itype: IterableType) -> None:
    first_value = 1
    second_value = 2
    third_value = 3
    functions = [
        lambda: throw(TestError),
        lambda: throw(TypeError),
        lambda: first_value,
        lambda: second_value,
        lambda: throw(ValueError),
        lambda: third_value,
        lambda: throw(ZeroDivisionError),
    ]

    caught_erroring_stream: stream[int] = stream(map(lambda f: f(), functions)).catch(
        TestError
    )
    # the first non-caught exception should be raised
    with pytest.raises(TypeError):
        alist_or_list(caught_erroring_stream, itype)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_handles_only_exceptions(itype: IterableType) -> None:
    only_caught_errors_stream = stream(
        map(lambda _: throw(TestError), range(2000))
    ).catch(TestError)
    # When upstream raise exceptions without yielding any element, listing the stream must return empty list, without recursion issue.
    assert alist_or_list(only_caught_errors_stream, itype=itype) == []


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_raises_stopiteration_on_only_exceptions(itype: IterableType) -> None:
    only_caught_errors_stream = stream(
        map(lambda _: throw(TestError), range(2000))
    ).catch(TestError)
    # When upstream raise exceptions without yielding any element, then the first call to `next` on a stream catching all errors should raise StopIteration.
    with pytest.raises(stopiteration_type(itype)):
        anext_or_next(
            aiter_or_iter(only_caught_errors_stream, itype=itype), itype=itype
        )


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_chained(itype: IterableType) -> None:
    """Catch chain should behave correctly."""
    iterator = aiter_or_iter(
        stream(map(throw, [TestError, ValueError])).catch(ValueError).catch(TestError),
        itype=itype,
    )
    # no non-raising elements so first next leads to StopIteration
    with pytest.raises(stopiteration_type(itype)):
        anext_or_next(iterator, itype=itype)


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_where_clause(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Catch does not catch if `where` not satisfied."""
    with pytest.raises(TypeError):
        alist_or_list(
            stream(map(throw, [ValueError, TypeError])).catch(
                Exception, where=adapt(lambda exc: "ValueError" in repr(exc))
            ),
            itype=itype,
        )


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_replace_with_non_none(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Catch should be able to yield a non-None replacement."""
    assert alist_or_list(
        stream(map(lambda n: 1 / n, [0, 1, 2, 4])).catch(
            ZeroDivisionError,
            replace=adapt(lambda e: float("inf")),
        ),
        itype=itype,
    ) == [float("inf"), 1, 0.5, 0.25]


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_replace_with_none(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Catch should be able to yield a None replacement."""
    assert alist_or_list(
        stream(map(lambda n: 1 / n, [0, 1, 2, 4])).catch(
            ZeroDivisionError,
            replace=adapt(lambda e: None),
        ),
        itype=itype,
    ) == [None, 1, 0.5, 0.25]


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_multiple_exception_types(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    errors_counter: Counter[Type[Exception]] = Counter()
    # `catch` should accept multiple types
    assert alist_or_list(
        stream(
            map(
                lambda n: 1 / n,  # potential ZeroDivisionError
                map(
                    throw_for_odd_func(TestError),  # potential TestError
                    map(
                        int,  # potential ValueError
                        "01234foo56789",
                    ),
                ),
            )
        ).catch(
            (ValueError, TestError, ZeroDivisionError),
            where=adapt(lambda exc: errors_counter.update([type(exc)]) is None),
        ),
        itype=itype,
    ) == list(map(lambda n: 1 / n, range(2, 10, 2)))
    # `catch` should accept multiple types
    assert errors_counter == {TestError: 5, ValueError: 3, ZeroDivisionError: 1}


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_do_side_effect(
    itype: IterableType, adapt: Callable[[Callable[[Any], Any]], Callable[[Any], Any]]
) -> None:
    """Do side effect should be correctly applied."""
    errors: List[Exception] = []
    # test sync/async combinations
    for where, do in (
        (identity, adapt(errors.append)),
        (adapt(identity), errors.append),
    ):
        # `do` side effect should be correctly applied
        errors.clear()
        assert alist_or_list(
            stream([0, 1, 0, 1, 0])
            .map(lambda n: round(1 / n, 2))
            .catch(ZeroDivisionError, where=where, do=do),
            itype=itype,
        ) == [1, 1]
        assert len(errors) == 3


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_stop_on_exception(itype: IterableType) -> None:
    """Test `stop` on exception."""
    assert alist_or_list(
        stream("01-3").map(int).catch(ValueError, stop=True),
        itype,
    ) == [0, 1]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_stop_on_exception_not_satisfying_where(itype: IterableType) -> None:
    """Test `stop` on exception not satisfying `where`."""
    assert alist_or_list(
        stream("01-3")
        .map(int)
        .catch(ValueError, where=lambda exc: False, stop=True)
        .catch(ValueError),
        itype,
    ) == [0, 1, 3]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_stop_with_replacement(itype: IterableType) -> None:
    """Test `stop` on exception, with replacement as last elem."""
    assert alist_or_list(
        stream("01-3").map(int).catch(ValueError, stop=True, replace=lambda exc: -1),
        itype,
    ) == [0, 1, -1]
