from typing import Any, Callable, List, Union

import pytest

from streamable import stream
from streamable._tools._func import asyncify
from tests.tools.error import TestError
from tests.tools.func import async_identity, identity, inverse, throw_func
from tests.tools.iter import (
    ITERABLE_TYPES,
    IterableType,
    alist_or_list,
    anext_or_next,
    aiter_or_iter,
)
from tests.tools.source import INTEGERS, ints


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_yields_elements_without_exceptions(itype: IterableType) -> None:
    s = ints.catch(Exception)
    assert alist_or_list(s, itype) == list(INTEGERS)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_ignores_matching_exceptions(itype: IterableType) -> None:
    s = stream("12-34").map(int).catch(ValueError)
    assert alist_or_list(s, itype) == [1, 2, 3, 4]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_raises_non_matching_exceptions(itype: IterableType) -> None:
    chars = "1-2034"
    s = stream(chars).map(int).do(inverse).catch(ValueError)
    stream_iter = aiter_or_iter(s, itype)
    assert anext_or_next(stream_iter, itype) == 1
    # ValueError caught for `int("-")`
    assert anext_or_next(stream_iter, itype) == 2
    with pytest.raises(ZeroDivisionError):
        anext_or_next(stream_iter, itype)
    assert anext_or_next(stream_iter, itype) == 3
    assert anext_or_next(stream_iter, itype) == 4


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_handles_thousands_of_consecutive_exceptions(itype: IterableType) -> None:
    s = stream(range(2000)).map(throw_func(TestError)).catch(TestError)
    assert alist_or_list(s, itype) == []


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_chained(itype: IterableType) -> None:
    chars = "1-2034"
    s = stream(chars).map(int).do(inverse).catch(ValueError).catch(ZeroDivisionError)
    assert alist_or_list(s, itype) == [1, 2, 3, 4]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_tuple(itype: IterableType) -> None:
    chars = "1-2034"
    s = stream(chars).map(int).do(inverse).catch((ValueError, ZeroDivisionError))
    assert alist_or_list(s, itype) == [1, 2, 3, 4]


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_where(itype: IterableType, adapt: Callable[[Any], Any]) -> None:
    inputs: List[Union[str, float]] = ["0", float("nan"), "1", "-", "2"]
    s: stream[int] = (
        stream(inputs)
        .map(int)
        .catch(ValueError, where=adapt(lambda e: "cannot convert float NaN" in str(e)))
    )
    stream_iter = aiter_or_iter(s, itype)
    assert anext_or_next(stream_iter, itype) == 0
    # caught ValueError caused by `int("nan")`
    assert anext_or_next(stream_iter, itype) == 1
    # but did not catch ValueError caused by `int("-")`
    with pytest.raises(
        ValueError, match=r"invalid literal for int\(\) with base 10: '-'"
    ):
        anext_or_next(stream_iter, itype)
    assert anext_or_next(stream_iter, itype) == 2


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_replace_with_none(
    itype: IterableType, adapt: Callable[[Any], Any]
) -> None:
    s = stream("1-23").map(int).catch(ValueError, replace=adapt(lambda e: None))
    elems: List[Union[int, None]] = alist_or_list(s, itype)
    assert elems == [1, None, 2, 3]


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_do(itype: IterableType, adapt: Callable[[Any], Any]) -> None:
    ints = [1, 0, 2, 3]
    errors: List[Exception] = []
    s = stream(ints).do(inverse).catch(ZeroDivisionError, do=adapt(errors.append))
    assert alist_or_list(s, itype) == [1, 2, 3]
    assert len(errors) == 1
    assert isinstance(errors[0], ZeroDivisionError)


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_with_do_raising(
    itype: IterableType, adapt: Callable[[Any], Any]
) -> None:
    """If `do` raises an exception, it is re-raised in place of the caught one."""
    ints = [0, 1, 0, 1, 0]
    errors: List[Exception] = []
    s = (
        stream(ints)
        .map(lambda n: round(1 / n, 2))
        .catch(ZeroDivisionError, do=throw_func(TestError))
        .catch(TestError, do=adapt(errors.append))
    )
    assert alist_or_list(s, itype) == [1, 1]
    assert len(errors) == ints.count(0)


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_with_replace_raising(
    itype: IterableType, adapt: Callable[[Any], Any]
) -> None:
    ints = [0, 1, 0, 1, 0]
    s = (
        stream(ints)
        .map(lambda n: round(1 / n, 2))
        .catch(ZeroDivisionError, replace=throw_func(TestError))
        .catch(TestError, replace=adapt(lambda e: None))
    )
    assert alist_or_list(s, itype) == [None, 1, None, 1, None]


@pytest.mark.parametrize("where", [identity, async_identity])
@pytest.mark.parametrize("adapt_do", [identity, asyncify])
@pytest.mark.parametrize("adapt_replace", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_sync_async_params_combination(
    where: Callable[[Exception], Any],
    adapt_do: Callable[[Callable[[Any], Any]], Callable[[Any], Any]],
    adapt_replace: Callable[[Callable[[Any], Any]], Callable[[Any], Any]],
    itype: IterableType,
) -> None:
    """Catch supports passing a mix of regular and async functions for where, do and replace."""
    errors: List[Exception] = []
    s = (
        stream([0, 1, 0, 1, 0])
        .do(inverse)
        .catch(
            ZeroDivisionError,
            where=where,
            do=adapt_do(errors.append),
            replace=adapt_replace(lambda e: None),
        )
    )
    assert alist_or_list(s, itype) == [None, 1, None, 1, None]
    assert len(errors) == 3


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_stop(itype: IterableType) -> None:
    s = stream("01-3").map(int).catch(ValueError, stop=True)
    assert alist_or_list(s, itype) == [0, 1]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_stop_with_falsy_where(itype: IterableType) -> None:
    """If `stop=True` but the exception does not satisfy `where`, the iteration should not stop."""
    s = (
        stream("01-3")
        .map(int)
        .catch(ValueError, where=lambda e: False, stop=True)
        .catch(ValueError)
    )
    assert alist_or_list(s, itype) == [0, 1, 3]


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_stop_with_where_raising(
    itype: IterableType, adapt: Callable[[Any], Any]
) -> None:
    """`stop=True` does not terminate the iteration if `where` raises."""
    ints = [0, 1, 0, 1, 0]
    s = (
        stream(ints)
        .map(lambda n: round(1 / n, 2))
        .catch(ZeroDivisionError, where=throw_func(TestError), stop=True)
        .catch(TestError)
    )
    assert alist_or_list(s, itype) == [1, 1]


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_stop_with_do_raising(
    itype: IterableType, adapt: Callable[[Any], Any]
) -> None:
    """`stop=True` terminates the iteration even if `do` raises."""
    ints = [1, 0, 1, 0]
    s = (
        stream(ints)
        .map(lambda n: round(1 / n, 2))
        .catch(ZeroDivisionError, do=throw_func(TestError), stop=True)
        .catch(TestError)
    )
    assert alist_or_list(s, itype) == [1]


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_stop_with_replace_raising(
    itype: IterableType, adapt: Callable[[Any], Any]
) -> None:
    """`stop=True` terminates the iteration even if `replace` raises."""
    ints = [1, 0, 1, 0]
    s = (
        stream(ints)
        .map(lambda n: round(1 / n, 2))
        .catch(ZeroDivisionError, replace=throw_func(TestError), stop=True)
        .catch(TestError)
    )
    assert alist_or_list(s, itype) == [1]


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_stop_with_replacement(itype: IterableType) -> None:
    """Test `stop` on exception, with replacement as last elem."""
    s = stream("01-3").map(int).catch(ValueError, stop=True, replace=lambda e: None)
    assert alist_or_list(s, itype) == [0, 1, None]


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_replace_occurs_after_do(
    itype: IterableType, adapt: Callable[[Any], Any]
) -> None:
    errors: List[Exception] = []
    s = (
        stream("-")
        .map(int)
        .catch(
            ValueError, do=adapt(errors.append), replace=adapt(lambda e: len(errors))
        )
    )
    assert alist_or_list(s, itype) == [len(errors)] == [1]


@pytest.mark.parametrize("adapt", [identity, asyncify])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_catch_do_occurs_after_where(
    itype: IterableType, adapt: Callable[[Any], Any]
) -> None:
    errors: List[Exception] = []
    s = (
        stream("-")
        .map(int)
        .catch(ValueError, where=adapt(lambda e: False), do=adapt(errors.append))
    )
    with pytest.raises(ValueError):
        anext_or_next(aiter_or_iter(s, itype), itype)

    assert errors == []
