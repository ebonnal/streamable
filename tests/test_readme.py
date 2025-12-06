from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import time
from datetime import timedelta
from typing import Iterator, List, Tuple, TypeVar

import pytest

from streamable._stream import stream

ints: stream[int] = stream(range(10))

inverses: stream[float] = ints.map(lambda n: round(1 / n, 2)).catch(ZeroDivisionError)

ints_by_parity: stream[List[int]] = ints.group(by=lambda n: n % 2)

three_ints_per_second: stream[int] = ints.throttle(5, per=timedelta(seconds=1))

T = TypeVar("T")


# fmt: off
def test_iterate() -> None:
    assert list(inverses) == [1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
    assert set(inverses) == {0.5, 1.0, 0.2, 0.33, 0.25, 0.17, 0.14, 0.12, 0.11}
    assert sum(inverses) == pytest.approx(2.82)
    assert max(inverses) == 1.0
    assert max(inverses) == 1.0
    inverses_iter = iter(inverses)
    assert next(inverses_iter) == 1.0
    assert next(inverses_iter) == 0.5

@pytest.mark.asyncio
async def test_aiterate() -> None:
    assert [inverse async for inverse in inverses] == [1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]

def test_map_example() -> None:
    integer_strings: stream[str] = ints.map(str)

    assert list(integer_strings) == ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']

def test_thread_concurrent_map_example() -> None:
    import httpx

    pokemon_names: stream[str] = (
        stream(range(1, 4))
        .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
        .map(httpx.get, concurrency=3)
        .map(httpx.Response.json)
        .map(lambda poke: poke["name"])
    )
    assert list(pokemon_names) == ['bulbasaur', 'ivysaur', 'venusaur']

def test_process_concurrent_map_example() -> None:
    with ProcessPoolExecutor(max_workers=10) as processes:
        state: List[int] = []
        # ints are mapped
        assert list(ints.map(state.append, concurrency=processes)) == [None] * 10
        # but the `state` of the main process is not mutated
        assert state == []

@pytest.mark.asyncio
async def test_async_amap_example() -> None:
    import httpx

    async with httpx.AsyncClient() as http:
        pokemon_names: stream[str] = (
            stream(range(1, 4))
            .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
            .map(http.get, concurrency=3)
            .map(httpx.Response.json)
            .map(lambda poke: poke["name"])
        )
        # consume as an AsyncIterable[str]
        assert [name async for name in pokemon_names] == ['bulbasaur', 'ivysaur', 'venusaur']

def test_starmap_example() -> None:
    from streamable import star

    zeros: stream[int] = (
        stream(enumerate(ints))
        .map(star(lambda index, integer: index - integer))
    )

    assert list(zeros) == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

def test_do_example() -> None:
    state: List[int] = []
    appending_ints: stream[int] = ints.do(state.append)

    assert list(appending_ints) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

def test_filter_example() -> None:
    even_ints: stream[int] = ints.filter(lambda n: n % 2 == 0)

    assert list(even_ints) == [0, 2, 4, 6, 8]

def test_throttle_example() -> None:
    from datetime import timedelta

    three_ints_per_second: stream[int] = ints.throttle(3, per=timedelta(seconds=1))

    start = time.perf_counter()
    # takes 3s: ceil(10 ints / 3 per_second) - 1
    assert list(three_ints_per_second) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    assert 2.99 < time.perf_counter() - start < 3.25

    ints_every_100_millis = (
        ints
        .throttle(1, per=timedelta(milliseconds=100))
    )

    start = time.perf_counter()
    # takes 900 millis: (10 ints - 1) * 100 millis
    assert list(ints_every_100_millis) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    assert 0.89 < time.perf_counter() - start < 0.95

def test_group_example() -> None:
    global ints_by_parity
    ints_by_5: stream[List[int]] = ints.group(5)

    assert list(ints_by_5) == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]

    ints_by_parity = ints.group(by=lambda n: n % 2)

    assert list(ints_by_parity) == [[0, 2, 4, 6, 8], [1, 3, 5, 7, 9]]

    from datetime import timedelta

    ints_within_1_sec: stream[List[int]] = (
        ints
        .throttle(2, per=timedelta(seconds=1))
        .group(interval=timedelta(seconds=0.99))
    )

    assert list(ints_within_1_sec) == [[0, 1, 2], [3, 4], [5, 6], [7, 8], [9]]

    ints_by_parity_by_2: stream[List[int]] = (
        ints
        .group(by=lambda n: n % 2, size=2)
    )

    assert list(ints_by_parity_by_2) == [[0, 2], [1, 3], [4, 6], [5, 7], [8], [9]]

def test_groupby_example() -> None:
    ints_by_parity: stream[Tuple[str, List[int]]] = (
        ints
        .groupby(lambda n: "odd" if n % 2 else "even")
    )

    assert list(ints_by_parity) == [("even", [0, 2, 4, 6, 8]), ("odd", [1, 3, 5, 7, 9])]

    from streamable import star

    counts_by_parity: stream[Tuple[str, int]] = (
        ints_by_parity
        .map(star(lambda parity, ints: (parity, len(ints))))
    )

    assert list(counts_by_parity) == [("even", 5), ("odd", 5)]

def test_flatten_example() -> None:
    global ints_by_parity
    even_then_odd_ints: stream[int] = ints_by_parity.flatten()

    assert list(even_then_odd_ints) == [0, 2, 4, 6, 8, 1, 3, 5, 7, 9]

    round_robined_ints: stream[int] = (
        stream([[0, 0], [1, 1, 1, 1], [2, 2]])
        .flatten(concurrency=2)
    )
    assert list(round_robined_ints) == [0, 1, 0, 1, 1, 2, 1, 2]

def test_catch_example() -> None:
    inverses: stream[float] = (
        ints
        .map(lambda n: round(1 / n, 2))
        .catch(ZeroDivisionError, replace=lambda e: float("inf"))
    )

    assert list(inverses) == [float("inf"), 1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]

    import httpx

    status_codes_ignoring_resolution_errors: stream[int] = (
        stream(["https://github.com", "https://foo.bar", "https://github.com/foo/bar"])
        .map(httpx.get, concurrency=2)
        .catch(httpx.ConnectError, when=lambda exception: "not known" in str(exception))
        .map(lambda response: response.status_code)
    )

    assert list(status_codes_ignoring_resolution_errors) == [200, 404]

    errors: List[Exception] = []

    def store_error(error: Exception) -> bool:
        errors.append(error)
        return True

    ints_in_string: stream[int] = (
        stream("012345foo6789")
        .map(int)
        .catch(ValueError, when=store_error)
    )

    assert list(ints_in_string) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    assert len(errors) == len("foo")

def test_truncate_example() -> None:
    five_first_ints: stream[int] = ints.truncate(5)

    assert list(five_first_ints) == [0, 1, 2, 3, 4]

    five_first_ints = ints.truncate(when=lambda n: n == 5)

    assert list(five_first_ints) == [0, 1, 2, 3, 4]

def test_skip_example() -> None:
    ints_after_five: stream[int] = ints.skip(5)

    assert list(ints_after_five) == [5, 6, 7, 8, 9]

    ints_after_five = ints.skip(until=lambda n: n >= 5)

    assert list(ints_after_five) == [5, 6, 7, 8, 9]

def test_distinct_example() -> None:
    distinct_chars: stream[str] = stream("foobarfooo").distinct()

    assert list(distinct_chars) == ["f", "o", "b", "a", "r"]

    strings_of_distinct_lengths: stream[str] = (
        stream(["a", "foo", "bar", "z"])
        .distinct(len)
    )

    assert list(strings_of_distinct_lengths) == ["a", "foo"]

    consecutively_distinct_chars: stream[str] = (
        stream("foobarfooo")
        .distinct(consecutive=True)
    )

    assert list(consecutively_distinct_chars) == ["f", "o", "b", "a", "r", "f", "o"]

def test_observe_example() -> None:
    assert list(ints.throttle(2, per=timedelta(seconds=1)).observe("ints")) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

def test_plus_example() -> None:
    assert list(ints + ints) == [0, 1, 2, 3 ,4, 5, 6, 7, 8, 9, 0, 1, 2, 3 ,4, 5, 6, 7, 8, 9]

def test_zip_example() -> None:
    from streamable import star

    cubes: stream[int] = (
        stream(zip(ints, ints, ints))  # stream[tuple[int, int, int]]
        .map(star(lambda a, b, c: a * b * c))  # stream[int]
    )

    assert list(cubes) == [0, 1, 8, 27, 64, 125, 216, 343, 512, 729]

def test_call_example() -> None:
    state: List[int] = []
    appending_ints: stream[int] = ints.do(state.append)
    assert appending_ints() is appending_ints
    assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

@pytest.mark.asyncio
async def test_await_example() -> None:
    state: List[int] = []
    appending_ints: stream[int] = ints.do(state.append)
    assert appending_ints is await appending_ints
    assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

def test_non_stopping_exceptions_example() -> None:
    from contextlib import suppress

    casted_ints: Iterator[int] = iter(stream("0123_56789").map(int).group(3).flatten())
    collected_casted_ints: List[int] = []

    with suppress(ValueError):
        collected_casted_ints.extend(casted_ints)
    assert collected_casted_ints == [0, 1, 2, 3]

    collected_casted_ints.extend(casted_ints)
    assert collected_casted_ints == [0, 1, 2, 3, 5, 6, 7, 8, 9]

@pytest.mark.asyncio
async def test_async_etl_example(tmp_path: Path) -> None: # pragma: no cover
    import csv
    from datetime import timedelta
    from itertools import count
    import httpx
    from streamable import stream

    with (tmp_path / "quadruped_pokemons.csv").open("w") as file:
        fields = ["id", "name", "is_legendary", "base_happiness", "capture_rate"]
        writer = csv.DictWriter(file, fields, extrasaction='ignore')
        writer.writeheader()

        async with httpx.AsyncClient() as http_client:
            pipeline = (
                # Infinite stream[int] of Pokemon ids starting from Pokémon #1: Bulbasaur
                stream(count(1))
                # Limit to 16 requests per second to be friendly to our fellow PokéAPI devs
                .throttle(16, per=timedelta(microseconds=1))
                # GET pokemons via 8 concurrent coroutines
                .map(lambda poke_id: f"https://pokeapi.co/api/v2/pokemon-species/{poke_id}")
                .map(http_client.get, concurrency=8)
                .do(httpx.Response.raise_for_status)
                .map(httpx.Response.json)
                # Stop the iteration when reaching the 1st pokemon of the 4th generation
                .truncate(when=lambda poke: poke["generation"]["name"] == "generation-iv")
                .observe("pokemons")
                # Keep only quadruped Pokemons
                .filter(lambda poke: poke["shape"]["name"] == "quadruped")
                # Write a batch of pokemons every 5 seconds to the CSV file
                .group(interval=timedelta(seconds=5))
                .do(writer.writerows)
                .flatten()
                .observe("written pokemons")
                # Catch exceptions and raises the 1st one at the end of the iteration
                .catch(Exception, finally_raise=True)
            )

            # Start a full async iteration
            await pipeline

def test_etl_example(tmp_path: Path) -> None: # pragma: no cover
    import csv
    from datetime import timedelta
    from itertools import count
    import httpx
    from streamable import stream

    with (tmp_path / "quadruped_pokemons.csv").open("w") as file:
        fields = ["id", "name", "is_legendary", "base_happiness", "capture_rate"]
        writer = csv.DictWriter(file, fields, extrasaction='ignore')
        writer.writeheader()
        with httpx.Client() as http_client:
            pipeline = (
                # Infinite stream[int] of Pokemon ids starting from Pokémon #1: Bulbasaur
                stream(count(1))
                # Limit to 16 requests per second to be friendly to our fellow PokéAPI devs
                .throttle(16, per=timedelta(microseconds=1))
                # GET pokemons concurrently using a pool of 8 threads
                .map(lambda poke_id: f"https://pokeapi.co/api/v2/pokemon-species/{poke_id}")
                .map(http_client.get, concurrency=8)
                .do(httpx.Response.raise_for_status)
                .map(httpx.Response.json)
                # Stop the iteration when reaching the 1st pokemon of the 4th generation
                .truncate(when=lambda poke: poke["generation"]["name"] == "generation-iv")
                .observe("pokemons")
                # Keep only quadruped Pokemons
                .filter(lambda poke: poke["shape"]["name"] == "quadruped")
                # Write a batch of pokemons every 5 seconds to the CSV file
                .group(interval=timedelta(seconds=5))
                .do(writer.writerows)
                .flatten()
                .observe("written pokemons")
                # Catch exceptions and raises the 1st one at the end of the iteration
                .catch(Exception, finally_raise=True)
            )

            # Start a full iteration
            pipeline()
# fmt: on
