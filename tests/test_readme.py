import asyncio
import time
import unittest
from datetime import timedelta
from typing import Iterator, List, Tuple, TypeVar

from streamable.stream import Stream

integers: Stream[int] = Stream(range(10))

inverses: Stream[float] = integers.map(lambda n: round(1 / n, 2)).catch(
    ZeroDivisionError
)

integers_by_parity: Stream[List[int]] = integers.group(by=lambda n: n % 2)

three_integers_per_second: Stream[int] = integers.throttle(5, per=timedelta(seconds=1))

T = TypeVar("T")


# fmt: off
class TestReadme(unittest.TestCase):
    def test_iterate(self) -> None:
        self.assertListEqual(
            list(inverses),
            [1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11],
        )
        self.assertSetEqual(
            set(inverses),
            {0.5, 1.0, 0.2, 0.33, 0.25, 0.17, 0.14, 0.12, 0.11},
        )
        self.assertAlmostEqual(sum(inverses), 2.82)
        self.assertEqual(max(inverses), 1.0)
        self.assertEqual(max(inverses), 1.0)
        inverses_iter = iter(inverses)
        self.assertEqual(next(inverses_iter), 1.0)
        self.assertEqual(next(inverses_iter), 0.5)

        async def main() -> List[float]:
            return [inverse async for inverse in inverses]

        assert asyncio.run(main()) == [1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]


    def test_map_example(self) -> None:
        integer_strings: Stream[str] = integers.map(str)

        assert list(integer_strings) == ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']

    def test_thread_concurrent_map_example(self) -> None:
        import requests

        pokemon_names: Stream[str] = (
            Stream(range(1, 4))
            .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
            .map(requests.get, concurrency=3)
            .map(requests.Response.json)
            .map(lambda poke: poke["name"])
        )
        assert list(pokemon_names) == ['bulbasaur', 'ivysaur', 'venusaur']

    def test_process_concurrent_map_example(self) -> None:
        state: List[int] = []
        # integers are mapped
        assert integers.map(state.append, concurrency=4, via="process").count() == 10
        # but the `state` of the main process is not mutated
        assert state == []

    def test_amap_example(self) -> None:
        import httpx

        pokemon_names: Stream[str] = (
            Stream(range(1, 4))
            .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
            .amap(httpx.AsyncClient().get, concurrency=3)
            .map(httpx.Response.json)
            .map(lambda poke: poke["name"])
        )

        assert list(pokemon_names) == ['bulbasaur', 'ivysaur', 'venusaur']

    def test_async_amap_example(self) -> None:
        import asyncio

        import httpx

        async def main() -> None:
            async with httpx.AsyncClient() as http_async_client:
                pokemon_names: Stream[str] = (
                    Stream(range(1, 4))
                    .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
                    .amap(http_async_client.get, concurrency=3)
                    .map(httpx.Response.json)
                    .map(lambda poke: poke["name"])
                )
                assert [name async for name in pokemon_names] == ['bulbasaur', 'ivysaur', 'venusaur']

        asyncio.run(main())

    def test_starmap_example(self) -> None:
        from streamable import star

        zeros: Stream[int] = (
            Stream(enumerate(integers))
            .map(star(lambda index, integer: index - integer))
        )

        assert list(zeros) == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    def test_foreach_example(self) -> None:
        state: List[int] = []
        appending_integers: Stream[int] = integers.foreach(state.append)

        assert list(appending_integers) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

    def test_filter_example(self) -> None:
        even_integers: Stream[int] = integers.filter(lambda n: n % 2 == 0)

        assert list(even_integers) == [0, 2, 4, 6, 8]

    def test_throttle_example(self) -> None:
        from datetime import timedelta

        three_integers_per_second: Stream[int] = integers.throttle(3, per=timedelta(seconds=1))

        start = time.perf_counter()
        # takes 3s: ceil(10 integers / 3 per_second) - 1
        assert list(three_integers_per_second) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        assert 2.99 < time.perf_counter() - start < 3.25

        integers_every_100_millis = (
            integers
            .throttle(1, per=timedelta(milliseconds=100))
        )

        start = time.perf_counter()
        # takes 900 millis: (10 integers - 1) * 100 millis
        assert list(integers_every_100_millis) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        assert 0.89 < time.perf_counter() - start < 0.95

    def test_group_example(self) -> None:
        global integers_by_parity
        integers_by_5: Stream[List[int]] = integers.group(size=5)

        assert list(integers_by_5) == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]

        integers_by_parity = integers.group(by=lambda n: n % 2)

        assert list(integers_by_parity) == [[0, 2, 4, 6, 8], [1, 3, 5, 7, 9]]

        from datetime import timedelta

        integers_within_1_sec: Stream[List[int]] = (
            integers
            .throttle(2, per=timedelta(seconds=1))
            .group(interval=timedelta(seconds=0.99))
        )

        assert list(integers_within_1_sec) == [[0, 1, 2], [3, 4], [5, 6], [7, 8], [9]]

        integers_by_parity_by_2: Stream[List[int]] = (
            integers
            .group(by=lambda n: n % 2, size=2)
        )

        assert list(integers_by_parity_by_2) == [[0, 2], [1, 3], [4, 6], [5, 7], [8], [9]]

    def test_groupby_example(self) -> None:
        integers_by_parity: Stream[Tuple[str, List[int]]] = (
            integers
            .groupby(lambda n: "odd" if n % 2 else "even")
        )

        assert list(integers_by_parity) == [("even", [0, 2, 4, 6, 8]), ("odd", [1, 3, 5, 7, 9])]

        from streamable import star

        counts_by_parity: Stream[Tuple[str, int]] = (
            integers_by_parity
            .map(star(lambda parity, ints: (parity, len(ints))))
        )

        assert list(counts_by_parity) == [("even", 5), ("odd", 5)]

    def test_flatten_example(self) -> None:
        global integers_by_parity
        even_then_odd_integers: Stream[int] = integers_by_parity.flatten()

        assert list(even_then_odd_integers) == [0, 2, 4, 6, 8, 1, 3, 5, 7, 9]

        mixed_ones_and_zeros: Stream[int] = (
            Stream([[0] * 4, [1] * 4])
            .flatten(concurrency=2)
        )
        assert list(mixed_ones_and_zeros) == [0, 1, 0, 1, 0, 1, 0, 1]

    def test_catch_example(self) -> None:
        inverses: Stream[float] = (
            integers
            .map(lambda n: round(1 / n, 2))
            .catch(ZeroDivisionError, replacement=float("inf"))
        )

        assert list(inverses) == [float("inf"), 1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]

        import requests
        from requests.exceptions import ConnectionError

        status_codes_ignoring_resolution_errors: Stream[int] = (
            Stream(["https://github.com", "https://foo.bar", "https://github.com/foo/bar"])
            .map(requests.get, concurrency=2)
            .catch(ConnectionError, when=lambda exception: "Max retries exceeded with url" in str(exception))
            .map(lambda response: response.status_code)
        )

        assert list(status_codes_ignoring_resolution_errors) == [200, 404]

        errors: List[Exception] = []

        def store_error(error: Exception) -> bool:
            errors.append(error)
            return True

        integers_in_string: Stream[int] = (
            Stream("012345foo6789")
            .map(int)
            .catch(ValueError, when=store_error)
        )

        assert list(integers_in_string) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        assert len(errors) == len("foo")

    def test_truncate_example(self) -> None:
        five_first_integers: Stream[int] = integers.truncate(5)

        assert list(five_first_integers) == [0, 1, 2, 3, 4]

        five_first_integers = integers.truncate(when=lambda n: n == 5)

        assert list(five_first_integers) == [0, 1, 2, 3, 4]

    def test_skip_example(self) -> None:
        integers_after_five: Stream[int] = integers.skip(5)

        assert list(integers_after_five) == [5, 6, 7, 8, 9]

        integers_after_five = integers.skip(until=lambda n: n >= 5)

        assert list(integers_after_five) == [5, 6, 7, 8, 9]

    def test_distinct_example(self) -> None:
        distinct_chars: Stream[str] = Stream("foobarfooo").distinct()

        assert list(distinct_chars) == ["f", "o", "b", "a", "r"]

        strings_of_distinct_lengths: Stream[str] = (
            Stream(["a", "foo", "bar", "z"])
            .distinct(len)
        )

        assert list(strings_of_distinct_lengths) == ["a", "foo"]

        consecutively_distinct_chars: Stream[str] = (
            Stream("foobarfooo")
            .distinct(consecutive_only=True)
        )

        assert list(consecutively_distinct_chars) == ["f", "o", "b", "a", "r", "f", "o"]

    def test_observe_example(self) -> None:
        assert list(integers.throttle(2, per=timedelta(seconds=1)).observe("integers")) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

    def test_plus_example(self) -> None:
        assert list(integers + integers) == [0, 1, 2, 3 ,4, 5, 6, 7, 8, 9, 0, 1, 2, 3 ,4, 5, 6, 7, 8, 9]

    def test_zip_example(self) -> None:
        from streamable import star

        cubes: Stream[int] = (
            Stream(zip(integers, integers, integers))  # Stream[Tuple[int, int, int]]
            .map(star(lambda a, b, c: a * b * c))  # Stream[int]
        )

        assert list(cubes) == [0, 1, 8, 27, 64, 125, 216, 343, 512, 729]

    def test_count_example(self) -> None:
        assert integers.count() == 10

    def test_acount_example(self) -> None:
        assert asyncio.run(integers.acount()) == 10

    def test_call_example(self) -> None:
        state: List[int] = []
        appending_integers: Stream[int] = integers.foreach(state.append)
        assert appending_integers() is appending_integers
        assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

    def test_await_example(self) -> None:
        async def test() -> None:
            state: List[int] = []
            appending_integers: Stream[int] = integers.foreach(state.append)
            appending_integers is await appending_integers
            assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        asyncio.run(test())

    def test_non_stopping_exceptions_example(self) -> None:
        from contextlib import suppress

        casted_ints: Iterator[int] = iter(Stream("0123_56789").map(int).group(3).flatten())
        collected_casted_ints: List[int] = []

        with suppress(ValueError):
            collected_casted_ints.extend(casted_ints)
        assert collected_casted_ints == [0, 1, 2, 3]

        collected_casted_ints.extend(casted_ints)
        assert collected_casted_ints == [0, 1, 2, 3, 5, 6, 7, 8, 9]

    def test_async_etl_example(self) -> None: # pragma: no cover
        # for mypy typing check only
        if not self:
            import asyncio
            import csv
            import itertools
            from datetime import timedelta

            import httpx

            from streamable import Stream

            async def main() -> None:
                with open("./quadruped_pokemons.csv", mode="w") as file:
                    fields = ["id", "name", "is_legendary", "base_happiness", "capture_rate"]
                    writer = csv.DictWriter(file, fields, extrasaction='ignore')
                    writer.writeheader()

                    async with httpx.AsyncClient() as http_async_client:
                        pipeline: Stream = (
                            # Infinite Stream[int] of Pokemon ids starting from Pokémon #1: Bulbasaur
                            Stream(itertools.count(1))
                            # Limit to 16 requests per second to be friendly to our fellow PokéAPI devs
                            .throttle(16, per=timedelta(seconds=1))
                            # GET pokemons via 8 concurrent coroutines
                            .map(lambda poke_id: f"https://pokeapi.co/api/v2/pokemon-species/{poke_id}")
                            .amap(http_async_client.get, concurrency=8)
                            .foreach(httpx.Response.raise_for_status)
                            .map(httpx.Response.json)
                            # Stop the iteration when reaching the 1st pokemon of the 4th generation
                            .truncate(when=lambda poke: poke["generation"]["name"] == "generation-iv")
                            .observe("pokemons")
                            # Keep only quadruped Pokemons
                            .filter(lambda poke: poke["shape"]["name"] == "quadruped")
                            .observe("quadruped pokemons")
                            # Catch errors due to None "generation" or "shape"
                            .catch(
                                TypeError,
                                when=lambda error: str(error) == "'NoneType' object is not subscriptable"
                            )
                            # Write a batch of pokemons every 5 seconds to the CSV file
                            .group(interval=timedelta(seconds=5))
                            .foreach(writer.writerows)
                            .flatten()
                            .observe("written pokemons")
                            # Catch exceptions and raises the 1st one at the end of the iteration
                            .catch(Exception, finally_raise=True)
                        )

                        await pipeline

            asyncio.run(main())

    def test_etl_example(self) -> None: # pragma: no cover
        # for mypy typing check only
        if not self:
            import csv
            import itertools
            from datetime import timedelta

            import requests

            from streamable import Stream

            with open("./quadruped_pokemons.csv", mode="w") as file:
                fields = ["id", "name", "is_legendary", "base_happiness", "capture_rate"]
                writer = csv.DictWriter(file, fields, extrasaction='ignore')
                writer.writeheader()
                pipeline = (
                    # Infinite Stream[int] of Pokemon ids starting from Pokémon #1: Bulbasaur
                    Stream(itertools.count(1))
                    # Limit to 16 requests per second to be friendly to our fellow PokéAPI devs
                    .throttle(16, per=timedelta(seconds=1))
                    # GET pokemons concurrently using a pool of 8 threads
                    .map(lambda poke_id: f"https://pokeapi.co/api/v2/pokemon-species/{poke_id}")
                    .map(requests.get, concurrency=8)
                    .foreach(requests.Response.raise_for_status)
                    .map(requests.Response.json)
                    # Stop the iteration when reaching the 1st pokemon of the 4th generation
                    .truncate(when=lambda poke: poke["generation"]["name"] == "generation-iv")
                    .observe("pokemons")
                    # Keep only quadruped Pokemons
                    .filter(lambda poke: poke["shape"]["name"] == "quadruped")
                    .observe("quadruped pokemons")
                    # Catch errors due to None "generation" or "shape"
                    .catch(
                        TypeError,
                        when=lambda error: str(error) == "'NoneType' object is not subscriptable"
                    )
                    # Write a batch of pokemons every 5 seconds to the CSV file
                    .group(interval=timedelta(seconds=5))
                    .foreach(writer.writerows)
                    .flatten()
                    .observe("written pokemons")
                    # Catch exceptions and raises the 1st one at the end of the iteration
                    .catch(Exception, finally_raise=True)
                )

                pipeline()
# fmt: on
