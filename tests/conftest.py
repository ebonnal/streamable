import asyncio
import json
import os
from enum import Enum
from http import HTTPStatus
from typing import Any, Dict, Generator, List

import httpx
import pytest
import respx


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
