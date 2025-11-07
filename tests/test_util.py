import asyncio

from streamable._util._functiontools import sidify, star
from streamable._util._futuretools import (
    ExecutorFIFOFutureResultCollection,
    FutureResult,
)


def test_sidify() -> None:
    def f(x: int) -> int:
        return x**2

    assert f(2) == 4
    assert sidify(f)(2) == 2

    # test decoration
    @sidify
    def g(x):
        return x**2

    assert g(2) == 2


def test_star() -> None:
    assert list(map(star(lambda i, n: i * n), enumerate(range(10)))) == list(
        map(lambda x: x**2, range(10))
    )

    @star
    def mul(a: int, b: int) -> int:
        return a * b

    assert list(map(mul, enumerate(range(10)))) == list(map(lambda x: x**2, range(10)))


def test_os_future_result_collection_anext():
    result = object()
    future_results = ExecutorFIFOFutureResultCollection()
    future_results.add(FutureResult(result))
    assert asyncio.run(future_results.__anext__()) == result
