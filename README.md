# ༄ `streamable`
> *Expressive iteration in Python*

[![Actions Status](https://github.com/ebonnal/streamable/workflows/unittest/badge.svg)](https://github.com/ebonnal/streamable/actions)
[![codecov](https://codecov.io/gh/ebonnal/streamable/graph/badge.svg?token=S62T0JQK9N)](https://codecov.io/gh/ebonnal/streamable)
[![Actions Status](https://github.com/ebonnal/streamable/workflows/typing/badge.svg)](https://github.com/ebonnal/streamable/actions)
[![Actions Status](https://github.com/ebonnal/streamable/workflows/lint/badge.svg)](https://github.com/ebonnal/streamable/actions)
[![Actions Status](https://github.com/ebonnal/streamable/workflows/PyPI/badge.svg)](https://pypi.org/project/streamable)

---

## TL;DR:
|||
|--|--|
|🔗 *Fluent*|Chain methods!|
|🇹 *Typed*|`Stream[T]` inherits from `Iterable[T]`, library **fully typed**, [`mypy`](https://github.com/python/mypy) it|
|💤 *Lazy*|Operations are **lazily evaluated** at iteration time|
|🔄 *Concurrent*|Seamlessly enjoy **threads** or `asyncio`-based concurrency|
|🛡️ *Robust*|Extensively unittested for **Python 3.7 to 3.12** with 100% coverage|
|🪶 *Light*|`pip install streamable` with **no additional dependencies**|

---


## 1. install

```bash
pip install streamable
```

## 2. import
```python
from streamable import Stream
```

## 3. init
Instantiate a `Stream[T]` from an `Iterable[T]`.

```python
integers: Stream[int] = Stream(range(10))
```

## 4. operate
- `Stream`s are ***immutable***: applying an operation returns a new stream.

- Operations are ***lazy***: only evaluated at iteration time.

```python
inverses: Stream[float] = (
    integers
    .map(lambda n: round(1 / n, 2))
    .catch(ZeroDivisionError)
)
```

## 5. iterate
- Iterate over a `Stream[T]` as you would over any other `Iterable[T]`.
- Source elements are ***processed on-the-fly***.

### collect it
```python
>>> list(inverses)
[1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
>>> set(inverses)
{0.5, 1.0, 0.2, 0.33, 0.25, 0.17, 0.14, 0.12, 0.11}
```

### reduce it
```python
>>> sum(integers)
2.82
>>> max(inverses)
1.0
>>> from functools import reduce
>>> reduce(..., inverses)
```

### loop it
```python
>>> for inverse in inverses:
>>>    ...
```

### next it
```python
>>> inverses_iter = iter(inverses)
>>> next(inverses_iter)
1.0
>>> next(inverses_iter)
0.5
```

---

# 📒 ***Operations***

## `.map`
> Applies a transformation on elements:
```python
negative_integer_strings: Stream[str] = integers.map(lambda n: -n).map(str)

assert list(integer_strings) == ['0', '-1', '-2', '-3', '-4', '-5', '-6', '-7', '-8', '-9']
```

### thread-based concurrency
> Applies the transformation concurrently using a thread pool of size `concurrency` (preserving the order):
```python
import requests

pokemon_names: Stream[str] = (
    Stream(range(1, 4))
    .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i + 1}")
    .map(requests.get, concurrency=3)
    .map(requests.Response.json)
    .map(lambda poke: poke["name"])
)
assert list(pokemon_names) == ['bulbasaur', 'ivysaur', 'venusaur']
```

### async-based concurrency
> The sibling operation called `.amap` applies an async transformation (preserving the order):
```python
import httpx
import asyncio

http_async_client = httpx.AsyncClient()

pokemon_names: Stream[str] = (
    Stream(range(1, 4))
    .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
    .amap(http_async_client.get, concurrency=3)
    .map(httpx.Response.json)
    .map(lambda poke: poke["name"])
)

assert list(pokemon_names) == ['bulbasaur', 'ivysaur', 'venusaur']
asyncio.run(http_async_client.aclose())
```


## `.foreach`
> Applies a side effect on elements:

```python
self_printing_integers: Stream[int] = integers.foreach(print)

assert list(self_printing_integers) == list(integers)  # triggers the printing
```

### thread-based concurrency
> Like `.map` it has an optional `concurrency: int` parameter.

### async-based concurrency
> Like `.map` it has a sibling operation `.aforeach` for async.

## `.filter`
> Keeps only the elements that satisfy a condition:

```python
pair_integers: Stream[int] = integers.filter(lambda n: n % 2 == 0)

assert list(pair_integers) == [0, 2, 4, 6, 8]
```

## `.throttle`

> Limits the rate at which elements are yielded:

```python
slow_integers: Stream[int] = integers.throttle(per_second=5)

assert list(slow_integers) == list(integers)  # takes 10 / 5 = 2 seconds
```

## `.group`

> Groups elements into `List`s:

```python
integers_5_by_5: Stream[List[int]] = integers.group(size=5)

assert list(integers_5_by_5) == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]
```
```python
integers_by_parity: Stream[List[int]] = integers.group(by=lambda n: n % 2)

assert list(integers_by_parity) == [[0, 2, 4, 6, 8], [1, 3, 5, 7, 9]]
```
```python
import timedelta from datetime

integers_within_1s: Stream[List[int]] = (
    integers
    .throttle(per_second=2)
    .group(interval=timedelta(seconds=1))
)

assert list(integers_within_1s) == [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]]
```

Mix `size`/`by`/`interval` parameters:
```python
integers_2_by_2_by_parity: Stream[List[int]] = integers.group(by=lambda n: n % 2, size=2)

assert list(integers_2_by_2_by_parity) == [[0, 2], [1, 3], [4, 6], [5, 7], [8], [9]]
```

## `.flatten`

> Ungroups elements assuming that they are `Iterable`s.

```python
pair_then_odd_integers: Stream[int] = integers_by_parity.flatten()

assert pair_then_odd_integers == [0, 2, 4, 6, 8, 1, 3, 5, 7, 9]
```

### thread-based concurrency
> Flattens `concurrency` iterables concurrently:
```python
letters_mix: Stream[str] = Stream(
    [
        Stream(["a"] * 5).throttle(per_second=10),
        Stream(["b"] * 5).throttle(per_second=10),
        Stream(["c"] * 5).throttle(per_second=10),
    ]
).flatten(concurrency=2)
assert list(letters_mix) == ['a', 'b', 'a', 'b', 'a', 'b', 'a', 'b', 'a', 'b', 'c', 'c', 'c', 'c', 'c']
```


## `.catch`

> Catches a given type of exceptions, and optionally yields a `replacement` value:

```python
inverses: Stream[float] = (
    integers
    .map(lambda n: round(1 / n, 2))
    .catch(ZeroDivisionError, replacement=float("inf"))
)

assert list(inverses) == [float("inf"), 1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
```

> You can specify an additional `when` condition for the catch:
```python
import requests
from requests.exceptions import ConnectionError

status_codes_ignoring_resolution_errors: Stream[int] = (
    Stream(["https://github.com", "https://foo.bar", "https://github.com/foo/bar"])
    .map(requests.get, concurrency=2)
    .catch(ConnectionError, when=lambda exception: "Failed to resolve" in str(exception))
    .map(lambda response: response.status_code)
)

assert list(status_codes_ignoring_resolution_errors) == [200, 404]
```

> It has an optional `finally_raise: bool` parameter to raise the first catched exception when iteration ends.

## `.truncate`
> Stops the iteration:
- after a given number of yielded elements:
```python
five_first_integers: Stream[int] = integers.truncate(5)

assert list(five_first_integers) == [0, 1, 2, 3, 4]
```

- as soon as a condition is satisfied:
```python
five_first_integers: Stream[int] = integers.truncate(when=lambda n: n == 5)

assert list(five_first_integers) == [0, 1, 2, 3, 4]
```

## `.observe`

> Logs the progress of iterations over this stream:

If you iterate on
```python
observed_slow_integers: Stream[int] = slow_integers.observe("integers")
```
you will get these logs:
```
INFO: [duration=0:00:00.502155 errors=0] 1 integers yielded
INFO: [duration=0:00:01.006336 errors=0] 2 integers yielded
INFO: [duration=0:00:02.011921 errors=0] 4 integers yielded
INFO: [duration=0:00:04.029666 errors=0] 8 integers yielded
INFO: [duration=0:00:05.039571 errors=0] 10 integers yielded
```

Note that the amount of logs will never be overwhelming because they are produced logarithmically e.g. the 11th log will be produced when the iteration reaches the 1024th element.

---

# 📦 ***Notes Box***
## Contribute
Please help me ! Feel very welcome to:
- [open issues](https://github.com/ebonnal/streamable/issues)
- [open pull requests](https://github.com/ebonnal/streamable/pulls)
- check [CONTRIBUTING.md](CONTRIBUTING.md)

## Extract-Transform-Load
ETL scripts (i.e. scripts fetching -> processing -> pushing data) can benefit from the expressivity of this library.

Here is an example that you can **copy-paste and try** (it only requires `requests`): it creates a CSV file containing all the 67 quadrupeds from the 1st, 2nd and 3rd generations of Pokémons (kudos to [PokéAPI](https://pokeapi.co/))
```python
import csv
from datetime import timedelta
import itertools
import requests
from streamable import Stream

with open("./quadruped_pokemons.csv", mode="w") as file:
    fields = ["id", "name", "is_legendary", "base_happiness", "capture_rate"]
    writer = csv.DictWriter(file, fields, extrasaction='ignore')
    writer.writeheader()
    (
        # Infinite Stream[int] of Pokemon ids starting from Pokémon #1: Bulbasaur
        Stream(itertools.count(1))
        # Limits to 16 requests per second to be friendly to our fellow PokéAPI devs
        .throttle(per_second=16)
        # GETs pokemons concurrently using a pool of 8 threads
        .map(lambda poke_id: f"https://pokeapi.co/api/v2/pokemon-species/{poke_id}")
        .map(requests.get, concurrency=8)
        .foreach(requests.Response.raise_for_status)
        .map(requests.Response.json)
        # Stops the iteration when reaching the 1st pokemon of the 4th generation
        .truncate(when=lambda poke: poke["generation"]["name"] == "generation-iv")
        .observe("pokemons")
        # Keeps only quadruped Pokemons
        .filter(lambda poke: poke["shape"]["name"] == "quadruped")
        .observe("quadruped pokemons")
        # Catches errors due to None "generation" or "shape"
        .catch(
            TypeError,
            when=lambda error: str(error) == "'NoneType' object is not subscriptable"
        )
        # Writes a batch of pokemons every 5 seconds to the CSV file
        .group(interval=timedelta(seconds=5))
        .foreach(writer.writerows)
        .flatten()
        .observe("written pokemons")
        # Catches any unexpected exception and raises at the end of the iteration
        .catch(finally_raise=True)
        # Actually triggers an iteration while previous lines define lazy operations
        .count()
    )
```

## CPU-bound tasks
For CPU-bound tasks, consider using the [`PyPy`](https://github.com/pypy/pypy) interpreter whose *Just In Time* (JIT) compilation should drastically improve performances !
([Few rough runtime orders of magnitude: CPython vs PyPy vs Java vs C vs Rust.](https://github.com/ebonnal/streamable/issues/10))

## change logging level
```python
logging.getLogger("streamable").setLevel(logging.WARNING)  # default is INFO
```

## visitor pattern
The `Stream` class exposes an `.accept` method and you can implement a [***visitor***](https://en.wikipedia.org/wiki/Visitor_pattern) by extending the `streamable.visitors.Visitor` abstract class:

```python
from streamable.visitors import Visitor

class DepthVisitor(Visitor[int]):
    def visit_stream(self, stream: Stream) -> int:
        if not stream.upstream:
            return 1
        return 1 + stream.upstream.accept(self)

def depth(stream: Stream) -> int:
    return stream.accept(DepthVisitor())

assert depth(Stream(range(10)).map(str).filter()) == 3
```

## as functions
The `Stream`'s methods are also exposed as functions:
```python
from streamable.functions import catch

inverse_integers: Iterator[int] = map(lambda n: 1 / n, range(10))
safe_inverse_integers: Iterator[int] = catch(inverse_integers, ZeroDivisionError)
```
