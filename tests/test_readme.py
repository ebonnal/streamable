import unittest
from typing import List

from streamable.stream import Stream

integers: Stream[int] = Stream(range(10))

inverses: Stream[float] = integers.map(lambda n: round(1 / n, 2)).catch(
    ZeroDivisionError
)

integers_by_parity: Stream[List[int]] = integers.group(by=lambda n: n % 2)

slow_integers: Stream[int] = integers.throttle(per_second=5)


class TestReadme(unittest.TestCase):
    def test_collect_it(self) -> None:
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

    def test_map_example(self) -> None:
        negative_integer_strings: Stream[str] = integers.map(lambda n: -n).map(str)
        assert list(negative_integer_strings) == [
            "0",
            "-1",
            "-2",
            "-3",
            "-4",
            "-5",
            "-6",
            "-7",
            "-8",
            "-9",
        ]

    def test_thread_concurrent_map_example(self) -> None:
        import requests

        pokemon_names: Stream[str] = (
            Stream(range(1, 4))
            .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
            .map(requests.get, concurrency=3)
            .map(requests.Response.json)
            .map(lambda poke: poke["name"])
        )
        assert list(pokemon_names) == ["bulbasaur", "ivysaur", "venusaur"]

    def test_async_concurrent_map_example(self) -> None:
        import asyncio

        import httpx

        http_async_client = httpx.AsyncClient()

        pokemon_names: Stream[str] = (
            Stream(range(1, 4))
            .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
            .amap(http_async_client.get, concurrency=3)
            .map(httpx.Response.json)
            .map(lambda poke: poke["name"])
        )

        assert list(pokemon_names) == ["bulbasaur", "ivysaur", "venusaur"]
        asyncio.run(http_async_client.aclose())

    def test_foreach_example(self) -> None:
        self_printing_integers: Stream[int] = integers.foreach(print)

        assert list(self_printing_integers) == list(integers)  # triggers the printing

    def test_filter_example(self) -> None:
        pair_integers: Stream[int] = integers.filter(lambda n: n % 2 == 0)

        assert list(pair_integers) == [0, 2, 4, 6, 8]

    def test_throttle_example(self) -> None:

        assert list(slow_integers) == list(integers)  # takes 10 / 5 = 2 seconds

    def test_group_example(self) -> None:
        global integers_by_parity
        integers_5_by_5: Stream[List[int]] = integers.group(size=5)

        assert list(integers_5_by_5) == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]

        assert list(integers_by_parity) == [[0, 2, 4, 6, 8], [1, 3, 5, 7, 9]]

        from datetime import timedelta

        integers_within_1s: Stream[List[int]] = integers.throttle(per_second=2).group(
            interval=timedelta(seconds=0.99)
        )

        assert list(integers_within_1s) == [[0, 1, 2], [3, 4], [5, 6], [7, 8], [9]]

        integers_2_by_2_by_parity: Stream[List[int]] = integers.group(
            by=lambda n: n % 2, size=2
        )

        assert list(integers_2_by_2_by_parity) == [
            [0, 2],
            [1, 3],
            [4, 6],
            [5, 7],
            [8],
            [9],
        ]

    def test_flatten_example(self) -> None:
        global integers_by_parity
        pair_then_odd_integers: Stream[int] = integers_by_parity.flatten()

        assert list(pair_then_odd_integers) == [0, 2, 4, 6, 8, 1, 3, 5, 7, 9]

    def test_concurrent_flatten_example(self) -> None:
        letters_mix: Stream[str] = Stream(
            [
                Stream(["a"] * 5).throttle(per_second=10),
                Stream(["b"] * 5).throttle(per_second=10),
                Stream(["c"] * 5).throttle(per_second=10),
            ]
        ).flatten(concurrency=2)
        assert list(letters_mix) == [
            "a",
            "b",
            "a",
            "b",
            "a",
            "b",
            "a",
            "b",
            "a",
            "b",
            "c",
            "c",
            "c",
            "c",
            "c",
        ]

    def test_catch_example(self) -> None:
        inverses: Stream[float] = integers.map(lambda n: round(1 / n, 2)).catch(
            ZeroDivisionError, replacement=float("inf")
        )

        assert list(inverses) == [
            float("inf"),
            1.0,
            0.5,
            0.33,
            0.25,
            0.2,
            0.17,
            0.14,
            0.12,
            0.11,
        ]

        import requests
        from requests.exceptions import ConnectionError

        status_codes_ignoring_resolution_errors: Stream[int] = (
            Stream(
                ["https://github.com", "https://foo.bar", "https://github.com/foo/bar"]
            )
            .map(requests.get, concurrency=2)
            .catch(
                ConnectionError,
                when=lambda exception: "Max retries exceeded" in str(exception),
            )
            .map(lambda response: response.status_code)
        )

        assert list(status_codes_ignoring_resolution_errors) == [200, 404]

    def test_truncate_example(self) -> None:
        five_first_integers: Stream[int] = integers.truncate(5)

        assert list(five_first_integers) == [0, 1, 2, 3, 4]

        five_first_integers = integers.truncate(when=lambda n: n == 5)

        assert list(five_first_integers) == [0, 1, 2, 3, 4]

    def test_observe_example(self) -> None:
        observed_slow_integers: Stream[int] = slow_integers.observe("integers")
