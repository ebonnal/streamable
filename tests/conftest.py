import datetime
import json
from typing import Any, Dict, Generator, List

import httpx
import pytest
import respx

from streamable import Stream
from streamable._utils._func import star
from streamable._utils._iter import (
    sync_to_async_iter,
)
from tests.utils import (
    async_identity,
    identity,
    src,
)


class CustomCallable:
    def __call__(self, *args, **kwds): ...

    def __repr__(self) -> str:
        return "CustomCallable()"


@pytest.fixture
def complex_stream() -> Stream[int]:
    return (
        Stream(src)
        .truncate(1024, when=lambda _: False)
        .atruncate(1024, when=async_identity)
        .skip(10)
        .askip(10)
        .skip(until=lambda _: True)
        .askip(until=async_identity)
        .distinct(lambda _: _)
        .adistinct(async_identity)
        .map(lambda i: (i,))
        .map(lambda i: (i,), concurrency=2)
        .filter(star(bool))
        .afilter(star(async_identity))
        .foreach(lambda _: _)
        .foreach(lambda _: _, concurrency=2)
        .aforeach(async_identity)
        .map(CustomCallable())
        .amap(async_identity)
        .group(100)
        .agroup(100)
        .groupby(len)
        .agroupby(async_identity)
        .map(star(lambda key, group: group))
        .observe("groups")
        .flatten(concurrency=4)
        .map(sync_to_async_iter)
        .aflatten(concurrency=4)
        .map(lambda _: 0)
        .throttle(64, per=datetime.timedelta(seconds=1))
        .observe("foos")
        .catch(Exception, finally_raise=True, when=identity)
        .acatch(Exception, finally_raise=True, when=async_identity)
        .catch((TypeError, ValueError, ZeroDivisionError))
        .acatch((TypeError, ValueError, ZeroDivisionError))
        .catch(TypeError, replace=identity, finally_raise=True)
        .acatch(TypeError, replace=async_identity, finally_raise=True)
    )


@pytest.fixture
def complex_stream_str() -> str:
    return """(
    Stream(range(0, 256))
    .truncate(count=1024, when=<lambda>)
    .atruncate(count=1024, when=async_identity)
    .skip(10, until=None)
    .askip(10, until=None)
    .skip(None, until=<lambda>)
    .askip(None, until=async_identity)
    .distinct(<lambda>, consecutive=False)
    .adistinct(async_identity, consecutive=False)
    .map(<lambda>, concurrency=1, ordered=True)
    .map(<lambda>, concurrency=2, ordered=True, via='thread')
    .filter(star(bool))
    .afilter(star(async_identity))
    .foreach(<lambda>, concurrency=1, ordered=True)
    .foreach(<lambda>, concurrency=2, ordered=True, via='thread')
    .aforeach(async_identity, concurrency=1, ordered=True)
    .map(CustomCallable(), concurrency=1, ordered=True)
    .amap(async_identity, concurrency=1, ordered=True)
    .group(size=100, by=None, interval=None)
    .agroup(size=100, by=None, interval=None)
    .groupby(len, size=None, interval=None)
    .agroupby(async_identity, size=None, interval=None)
    .map(star(<lambda>), concurrency=1, ordered=True)
    .observe('groups')
    .flatten(concurrency=4)
    .map(SyncToAsyncIterator, concurrency=1, ordered=True)
    .aflatten(concurrency=4)
    .map(<lambda>, concurrency=1, ordered=True)
    .throttle(64, per=datetime.timedelta(seconds=1))
    .observe('foos')
    .catch(Exception, when=identity, finally_raise=True)
    .acatch(Exception, when=async_identity, finally_raise=True)
    .catch((TypeError, ValueError, ZeroDivisionError), when=None, finally_raise=False)
    .acatch((TypeError, ValueError, ZeroDivisionError), when=None, finally_raise=False)
    .catch(TypeError, when=None, replace=identity, finally_raise=True)
    .acatch(TypeError, when=None, replace=async_identity, finally_raise=True)
)"""


@pytest.fixture(autouse=True)
def mock_httpx() -> Generator:
    with open("tests/pokemons.json") as pokemon_sample:
        POKEMONS: List[Dict[str, Any]] = json.loads(pokemon_sample.read())
    with respx.mock:
        for i, pokemon in enumerate(POKEMONS):
            respx.get(f"https://pokeapi.co/api/v2/pokemon-species/{i + 1}").mock(
                return_value=httpx.Response(200, json=pokemon)
            )
        respx.get("https://github.com/foo/bar").mock(return_value=httpx.Response(404))
        respx.get("https://github.com").mock(return_value=httpx.Response(200))
        respx.get("https://foo.bar").mock(
            side_effect=httpx.ConnectError(
                "[Errno 8] nodename nor servname provided, or not known"
            )
        )
        yield
