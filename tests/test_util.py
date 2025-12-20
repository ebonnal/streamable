import asyncio
import pytest
from streamable._tools._func import sidify, star
from streamable._tools._future import (
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


@pytest.mark.asyncio
async def test_star() -> None:
    @star
    def add(a: int, b: int) -> int:
        return a + b

    assert add((2, 5)) == 7

    assert star(lambda a, b: a + b)((2, 5)) == 7

    @star
    async def sleepy_add(a: int, b: int) -> int:
        await asyncio.sleep(1)
        return a + b

    assert (await sleepy_add((2, 5))) == 7

    async def sleepy_add_(a: int, b: int) -> int:
        await asyncio.sleep(1)
        return a + b

    assert (await star(sleepy_add_)((2, 5))) == 7


def test_os_future_result_collection_anext():
    result = object()
    future_results = ExecutorFIFOFutureResultCollection()
    future_results.add(FutureResult(result))
    assert asyncio.run(future_results.__anext__()) == result
