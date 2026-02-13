from pathlib import Path
from datetime import timedelta
import httpx

import pytest


def test_etl_example(tmp_path: Path) -> None:  # pragma: no cover
    import csv
    from itertools import count
    from streamable import stream

    with open(tmp_path / "quadruped_pokemons.csv", mode="w") as file:
        fields = ["id", "name", "is_legendary", "base_happiness", "capture_rate"]
        writer = csv.DictWriter(file, fields, extrasaction="ignore")
        writer.writeheader()

        pipeline = (
            # Infinite stream[int] of Pokemon ids starting from Pokémon #1: Bulbasaur
            stream(count(1))
            # Limit to 16 requests per second to be friendly to our fellow PokéAPI devs
            .throttle(16, per=timedelta(milliseconds=1))
            # GET pokemons concurrently using a pool of 8 threads
            .map(lambda poke_id: f"https://pokeapi.co/api/v2/pokemon-species/{poke_id}")
            .map(httpx.Client().get, concurrency=8)
            .do(httpx.Response.raise_for_status)
            .map(httpx.Response.json)
            # Stop when reaching the 1st pokemon of the 4th generation
            .take(until=lambda poke: poke["generation"]["name"] == "generation-iv")
            .observe("pokemons")
            # Keep only quadruped Pokemons
            .filter(lambda poke: poke["shape"]["name"] == "quadruped")
            # Write a batch of pokemons every 5 seconds to the CSV file
            .group(within=timedelta(seconds=5))
            .do(writer.writerows)
            .flatten()
            .observe("written pokemons")
        )

        # Call the stream to consume it (as an Iterable)
        # without collecting its elements
        pipeline()


@pytest.mark.asyncio
async def test_async_etl_example(tmp_path: Path) -> None:  # pragma: no cover
    import csv
    from itertools import count
    from streamable import stream

    with open(tmp_path / "quadruped_pokemons.csv", mode="w") as file:
        fields = ["id", "name", "is_legendary", "base_happiness", "capture_rate"]
        writer = csv.DictWriter(file, fields, extrasaction="ignore")
        writer.writeheader()

        pipeline = (
            # Infinite stream[int] of Pokemon ids starting from Pokémon #1: Bulbasaur
            stream(count(1))
            # Limit to 16 requests per second to be friendly to our fellow PokéAPI devs
            .throttle(16, per=timedelta(milliseconds=1))
            # GET pokemons via 8 concurrent coroutines
            .map(lambda poke_id: f"https://pokeapi.co/api/v2/pokemon-species/{poke_id}")
            .map(httpx.AsyncClient().get, concurrency=8)
            .do(httpx.Response.raise_for_status)
            .map(httpx.Response.json)
            # Stop when reaching the 1st pokemon of the 4th generation
            .take(until=lambda poke: poke["generation"]["name"] == "generation-iv")
            .observe("pokemons")
            # Keep only quadruped Pokemons
            .filter(lambda poke: poke["shape"]["name"] == "quadruped")
            # Write a batch of pokemons every 5 seconds to the CSV file
            .group(within=timedelta(seconds=5))
            .do(writer.writerows)
            .flatten()
            .observe("written pokemons")
        )

        # await the stream to consume it (as an AsyncIterable)
        # without collecting its elements
        await pipeline


def test_etl_dlt(tmp_path: Path) -> None:
    from datetime import timedelta
    from http import HTTPStatus
    from itertools import count

    import dlt
    from httpx import AsyncClient, Response, HTTPStatusError
    from dlt.destinations import filesystem
    from streamable import stream

    def not_found(e: HTTPStatusError) -> bool:
        return e.response.status_code == HTTPStatus.NOT_FOUND

    @dlt.resource
    def pokemons(concurrency: int, per_second: int) -> stream[dict]:
        """
        Ingest Pokémons from the PokéAPI, stops on first 404.
        """
        return (
            stream(count(1))
            .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
            .throttle(per_second, per=timedelta(seconds=1))
            .map(AsyncClient().get, concurrency=concurrency, as_completed=True)
            .do(Response.raise_for_status)
            .catch(HTTPStatusError, where=not_found, stop=True)
            .map(Response.json)
            .observe("pokemons")
        )

    dlt.pipeline(
        pipeline_name="ingest_pokeapi",
        destination=filesystem(str(tmp_path / "deltalake")),
        dataset_name="pokeapi",
    ).run(
        pokemons(concurrency=8, per_second=32),
        table_format="delta",
        columns={"color__name": {"partition": True}},
    )

    import polars as pl

    assert len(pl.read_delta(tmp_path / "deltalake" / "pokeapi" / "pokemons")) == 511


# fmt: on
