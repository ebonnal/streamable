import json
from typing import Any, Dict, List, Union
from unittest.mock import Mock
import httpx
import requests

with open("tests/pokemons.json") as pokemon_sample:
    POKEMONS: List[Dict[str, Any]] = json.loads(pokemon_sample.read())


def get_poke(url: str) -> Union[requests.Response, httpx.Response]:
    poke_id = int(url.split("/")[-1])
    response = Mock()
    response.text = json.dumps(POKEMONS[poke_id - 1])
    response.content = response.text
    response.status_code = 200
    return response


async def async_get_poke(url: str) -> Union[requests.Response, httpx.Response]:
    return get_poke(url)
