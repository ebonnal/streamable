from collections import Counter
from typing import (
    List,
    Type,
)

import pytest

from streamable import stream
from streamable._utils._func import asyncify
from tests.utils import (
    ITERABLE_TYPES,
    IterableType,
    TestError,
    anext_or_next,
    bi_iterable_to_iter,
    identity,
    ints_src,
    stopiteration_type,
    throw,
    throw_for_odd_func,
    to_list,
)


@pytest.mark.parametrize(
    "itype, adapt",
    ((itype, adapt) for adapt in (identity, asyncify) for itype in ITERABLE_TYPES),
)
def test_catch(itype: IterableType, adapt) -> None:
    # `catch` should yield elements in exception-less scenarios
    assert to_list(stream(ints_src).catch(Exception), itype=itype) == list(ints_src)

    def fn(i):
        return i / (3 - i)

    stream_ = stream(ints_src).map(fn)
    safe_src = list(ints_src)
    del safe_src[3]
    # If the exception type matches the `error_type`, then the impacted element should be ignored.
    assert to_list(stream_.catch(ZeroDivisionError), itype=itype) == list(
        map(fn, safe_src)
    )
    # If a non-caught exception type occurs, then it should be raised.
    with pytest.raises(ZeroDivisionError):
        to_list(stream_.catch(TestError), itype=itype)

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
        to_list(caught_erroring_stream, itype)

    only_caught_errors_stream = stream(
        map(lambda _: throw(TestError), range(2000))
    ).catch(TestError)
    # When upstream raise exceptions without yielding any element, listing the stream must return empty list, without recursion issue.
    assert to_list(only_caught_errors_stream, itype=itype) == []
    # When upstream raise exceptions without yielding any element, then the first call to `next` on a stream catching all errors should raise StopIteration.
    with pytest.raises(stopiteration_type(itype)):
        anext_or_next(bi_iterable_to_iter(only_caught_errors_stream, itype=itype))

    # `catch`s chain should behave correctly
    iterator = bi_iterable_to_iter(
        stream(map(throw, [TestError, ValueError])).catch(ValueError).catch(TestError),
        itype=itype,
    )
    # no non-raising elements so first next leads to StopIteration
    with pytest.raises(stopiteration_type(itype)):
        anext_or_next(iterator)

    # `catch` does not catch if `where` not satisfied
    with pytest.raises(TypeError):
        to_list(
            stream(map(throw, [ValueError, TypeError])).catch(
                Exception, where=adapt(lambda exc: "ValueError" in repr(exc))
            ),
            itype=itype,
        )
    # `catch` should be able to yield a non-None replacement
    assert to_list(
        stream(map(lambda n: 1 / n, [0, 1, 2, 4])).catch(
            ZeroDivisionError,
            replace=adapt(lambda e: float("inf")),
        ),
        itype=itype,
    ) == [float("inf"), 1, 0.5, 0.25]
    # `catch` should be able to yield a None replacement
    assert to_list(
        stream(map(lambda n: 1 / n, [0, 1, 2, 4])).catch(
            ZeroDivisionError,
            replace=adapt(lambda e: None),
        ),
        itype=itype,
    ) == [None, 1, 0.5, 0.25]

    errors_counter: Counter[Type[Exception]] = Counter()
    # `catch` should accept multiple types
    assert to_list(
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

    errors: List[Exception] = []
    # test sync/async combinations
    for where, do in (
        (identity, adapt(errors.append)),
        (adapt(identity), errors.append),
    ):
        # `do` side effect should be correctly applied
        errors.clear()
        assert to_list(
            stream([0, 1, 0, 1, 0])
            .map(lambda n: round(1 / n, 2))
            .catch(ZeroDivisionError, where=where, do=do),
            itype=itype,
        ) == [1, 1]
        assert len(errors) == 3

    # test `stop` on exception
    assert to_list(
        stream("01-3").map(int).catch(ValueError, stop=True),
        itype,
    ) == [0, 1]
    # test `stop` on exception not satisfying `where`
    assert to_list(
        stream("01-3")
        .map(int)
        .catch(ValueError, where=lambda exc: False, stop=True)
        .catch(ValueError),
        itype,
    ) == [0, 1, 3]
    # test `stop` on exception, with replacement as last elem
    assert to_list(
        stream("01-3").map(int).catch(ValueError, stop=True, replace=lambda exc: -1),
        itype,
    ) == [0, 1, -1]
