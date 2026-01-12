import asyncio
import pytest
from streamable._tools._func import sidify, star
from streamable._tools._logging import logfmt_str_escape


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


def test_logfmt_str_escape():
    """Test logfmt string escaping."""
    assert logfmt_str_escape("") == '""'
    assert logfmt_str_escape("ints") == "ints"
    assert logfmt_str_escape("in ts") == '"in ts"'
    assert logfmt_str_escape("in\\ts") == r'"in\\ts"'
    assert logfmt_str_escape('"ints"') == r'"\"ints\""'
