import asyncio
import gc
import json
import os
from enum import Enum
from http import HTTPStatus
from typing import Any, Dict, Generator, List

import httpx
import pytest
import respx

from streamable._tools._async import safe_get_event_loop


class TestEventLoop(Enum):
    UVLOOP = "uvloop"
    ASYNCIO = "asyncio"

    @classmethod
    def current(cls) -> "TestEventLoop":
        return cls(os.environ.get("STREAMABLE_TEST_LOOP", "asyncio"))


if TestEventLoop.current() is TestEventLoop.UVLOOP:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


@pytest.fixture(autouse=True)
def mock_pokeapi() -> Generator:
    with open("tests/pokemons.json") as pokemon_sample:
        POKEMONS: List[Dict[str, Any]] = json.loads(pokemon_sample.read())
    with respx.mock:
        respx.get("https://pokeapi.co/api/v2/pokemon-species/0").mock(
            return_value=httpx.Response(HTTPStatus.NOT_FOUND, text="")
        )
        for i, pokemon in enumerate(POKEMONS):
            respx.get(f"https://pokeapi.co/api/v2/pokemon-species/{i + 1}").mock(
                return_value=httpx.Response(HTTPStatus.OK, json=pokemon)
            )
        respx.get(
            f"https://pokeapi.co/api/v2/pokemon-species/{len(POKEMONS) + 1}"
        ).mock(return_value=httpx.Response(HTTPStatus.NOT_FOUND, text=""))
        yield


@pytest.fixture(autouse=True)
def gc_disabled() -> Generator[None, None, None]:
    # cleanup must come from refcounting alone, a GC collection cannot hide a ref cycle
    gc.disable()
    yield
    gc.enable()
    gc.collect()


@pytest.fixture(autouse=True)
def loop_reset(request: pytest.FixtureRequest) -> Generator[None, None, None]:
    if "event_loop" not in request.fixturenames:
        loop = safe_get_event_loop()
        if loop is not None:
            loop.close()
        asyncio.set_event_loop(None)
    yield
