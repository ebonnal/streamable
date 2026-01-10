import json
from typing import Any, Dict, Generator, List

import httpx
import pytest
import respx


@pytest.fixture(autouse=True)
def mock_pokeapi() -> Generator:
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
        yield
