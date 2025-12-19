import datetime
import json
from typing import Any, Dict, Generator, List

import httpx
import pytest
import respx

from streamable import stream
from streamable._utils._func import star
from streamable._utils._iter import (
    sync_to_async_iter,
)
from tests.utils import (
    async_identity,
    identity,
    ints_src,
)


class CustomCallable:
    def __call__(self, *args, **kwds): ...

    def __repr__(self) -> str:
        return "CustomCallable()"


@pytest.fixture
def complex_stream() -> stream:
    return (
        stream(ints_src)
        .take(1024)
        .take(1024)
        .take(until=lambda _: False)
        .take(until=async_identity)
        .skip(10)
        .skip(until=lambda _: True)
        .skip(until=async_identity)
        .map(lambda i: (i,))
        .map(lambda i: (i,), concurrency=2)
        .filter(star(bool))
        .filter(star(async_identity))
        .do(lambda _: _)
        .do(lambda _: _, concurrency=2)
        .do(async_identity)
        .map(CustomCallable())
        .map(async_identity)
        .group(100)
        .group(100, by=async_identity)
        .map(star(lambda key, group: group))
        .observe("groups")
        .flatten(concurrency=4)
        .map(sync_to_async_iter)
        .flatten(concurrency=4)
        .map(lambda _: 0)
        .throttle(64, per=datetime.timedelta(seconds=1))
        .observe("foos", every=10)
        .catch(Exception, where=identity, do=identity, stop=True)
        .catch(Exception, where=async_identity)
        .catch((TypeError, ValueError, ZeroDivisionError))
        .catch((TypeError, ValueError, ZeroDivisionError))
        .catch(TypeError, replace=identity, stop=True)
        .catch(TypeError, replace=async_identity)
    )


@pytest.fixture
def complex_stream_str() -> str:
    return """(
    stream(range(0, 256))
    .take(until=1024)
    .take(until=1024)
    .take(until=<lambda>)
    .take(until=async_identity)
    .skip(until=10)
    .skip(until=<lambda>)
    .skip(until=async_identity)
    .map(<lambda>, concurrency=1, ordered=True)
    .map(<lambda>, concurrency=2, ordered=True)
    .filter(star(bool))
    .filter(star(async_identity))
    .do(<lambda>, concurrency=1, ordered=True)
    .do(<lambda>, concurrency=2, ordered=True)
    .do(async_identity, concurrency=1, ordered=True)
    .map(CustomCallable(), concurrency=1, ordered=True)
    .map(async_identity, concurrency=1, ordered=True)
    .group(up_to=100, every=None, by=None)
    .group(up_to=100, every=None, by=async_identity)
    .map(star(<lambda>), concurrency=1, ordered=True)
    .observe('groups', every=None)
    .flatten(concurrency=4)
    .map(SyncToAsyncIterator, concurrency=1, ordered=True)
    .flatten(concurrency=4)
    .map(<lambda>, concurrency=1, ordered=True)
    .throttle(64, per=datetime.timedelta(seconds=1))
    .observe('foos', every=10)
    .catch(Exception, where=identity, do=identity, replace=None, stop=True)
    .catch(Exception, where=async_identity, do=None, replace=None, stop=False)
    .catch((TypeError, ValueError, ZeroDivisionError), where=None, do=None, replace=None, stop=False)
    .catch((TypeError, ValueError, ZeroDivisionError), where=None, do=None, replace=None, stop=False)
    .catch(TypeError, where=None, do=None, replace=identity, stop=True)
    .catch(TypeError, where=None, do=None, replace=async_identity, stop=False)
)"""


@pytest.fixture(autouse=True)
def mock_httpx() -> Generator:
    with open("tests/pokemons.json") as pokemon_sample:
        POKEMONS: List[Dict[str, Any]] = json.loads(pokemon_sample.read())
    with respx.mock:
        respx.get("https://pokeapi.co/api/v2/pokemon-species/0").mock(
            return_value=httpx.Response(404, text="")
        )
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
