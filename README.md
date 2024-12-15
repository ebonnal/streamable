# ༄ `streamable`

> Stream-like manipulation of iterables

A `Stream[T]` decorates an `Iterable[T]` with a **fluent interface** enabling the chaining of lazy operations.

---

[![codecov](https://codecov.io/gh/ebonnal/streamable/graph/badge.svg?token=S62T0JQK9N)](https://codecov.io/gh/ebonnal/streamable)
[![unittest](https://github.com/ebonnal/streamable/actions/workflows/unittest.yml/badge.svg?branch=main)](https://github.com/ebonnal/streamable/actions)
[![typing](https://github.com/ebonnal/streamable/actions/workflows/typing.yml/badge.svg?branch=main)](https://github.com/ebonnal/streamable/actions)
[![lint](https://github.com/ebonnal/streamable/actions/workflows/lint.yml/badge.svg?branch=main)](https://github.com/ebonnal/streamable/actions)
[![PyPI](https://github.com/ebonnal/streamable/actions/workflows/pypi.yml/badge.svg?branch=main)](https://pypi.org/project/streamable)


|||
|--|--|
|🔗 *Fluent*|chain methods!|
|🇹 *Typed*|**type-annotated** and [`mypy`](https://github.com/python/mypy)able|
|💤 *Lazy*|operations are **lazily evaluated** at iteration time|
|🔄 *Concurrent*|via **threads** or **processes** or `asyncio`|
|🛡️ *Robust*|unit-tested for **Python 3.7 to 3.14** with 100% coverage|
|🪶 *Minimalist*|`pip install streamable` with **no additional dependencies**|

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
>>> sum(inverses)
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

assert list(negative_integer_strings) == ['0', '-1', '-2', '-3', '-4', '-5', '-6', '-7', '-8', '-9']
```

### thread-based concurrency

> Applies the transformation via `concurrency` threads:

```python
import requests

pokemon_names: Stream[str] = (
    Stream(range(1, 4))
    .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
    .map(requests.get, concurrency=3)
    .map(requests.Response.json)
    .map(lambda poke: poke["name"])
)
assert list(pokemon_names) == ['bulbasaur', 'ivysaur', 'venusaur']
```

> Preserves the upstream order by default (FIFO), but you can set `ordered=False` for *First Done First Out*.

> `concurrency` is also the size of the buffer containing not-yet-yielded results. **If the buffer is full, the iteration over the upstream is paused** until a result is yielded from the buffer.


### process-based concurrency

> Set `via="process"`:

```python
if __name__ == "__main__":
    state: List[int] = []
    n_integers: int = integers.map(state.append, concurrency=4, via="process").count()
    assert n_integers == 10
    assert state == [] # main process's state not mutated
```

### async-based concurrency

> The sibling operation `.amap` applies an async function:

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
asyncio.get_event_loop().run_until_complete(http_async_client.aclose())
```

### starmap

> The `star` function decorator transforms a function that takes several positional arguments into a function that takes a tuple:

```python
from streamable import star

zeros: Stream[int] = (
    Stream(enumerate(integers))
    .map(star(lambda index, integer: index - integer))
)

assert list(zeros) == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
```

> Also convenient with `.foreach`, `.filter`, ...

## `.foreach`

> Applies a side effect on elements:

```python
self_printing_integers: Stream[int] = integers.foreach(print)

assert list(self_printing_integers) == list(integers)  # triggers the printing
```

### thread-based concurrency

> Like `.map` it has an optional `concurrency` parameter.

### process-based concurrency

> Like for `.map`, set the parameter `via="process"`.

### async-based concurrency

> Like `.map` it has a sibling `.aforeach` operation for async.

## `.filter`

> Keeps only the elements that satisfy a condition:

```python
pair_integers: Stream[int] = integers.filter(lambda n: n % 2 == 0)

assert list(pair_integers) == [0, 2, 4, 6, 8]
```

## `.throttle`

> Limits the number of yields `per_second`/`per_minute`/`per_hour`:

```python
slow_integers: Stream[int] = integers.throttle(per_second=5)

assert list(slow_integers) == list(integers)  # takes 10 / 5 = 2 seconds
```

> and/or ensure a minimum time `interval` separates successive yields:

```python
from datetime import timedelta

slow_integers = integers.throttle(interval=timedelta(milliseconds=100))

assert list(slow_integers) == list(integers)  # takes 10 * 0.1 = 1 second
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
from datetime import timedelta

integers_within_1s: Stream[List[int]] = (
    integers
    .throttle(per_second=2)
    .group(interval=timedelta(seconds=0.99))
)

assert list(integers_within_1s) == [[0, 1, 2], [3, 4], [5, 6], [7, 8], [9]]
```

> Mix `size`/`by`/`interval` parameters:
```python
integers_2_by_2_by_parity: Stream[List[int]] = integers.group(by=lambda n: n % 2, size=2)

assert list(integers_2_by_2_by_parity) == [[0, 2], [1, 3], [4, 6], [5, 7], [8], [9]]
```

### `.groupby`

> Alternative to `.group` that yields `(key, group)` tuples:
```python
integers_by_parity: Stream[Tuple[str, List[int]]] = integers.groupby(lambda n: "odd" if n % 2 else "pair")

assert list(integers_by_parity) == [("pair", [0, 2, 4, 6, 8]), ("odd", [1, 3, 5, 7, 9])]
```

> [!TIP]
> *"star map"* over the resulting tuples:

```python
from streamable import star

counts_by_parity: Stream[Tuple[str, int]] = integers_by_parity.map(star(lambda parity, ints: (parity, len(ints))))

assert list(counts_by_parity) == [("pair", 5), ("odd", 5)]
```

## `.flatten`

> Ungroups elements assuming that they are `Iterable`s:

```python
pair_then_odd_integers: Stream[int] = integers_by_parity.flatten()

assert list(pair_then_odd_integers) == [0, 2, 4, 6, 8, 1, 3, 5, 7, 9]
```

### thread-based concurrency

> Flattens `concurrency` iterables concurrently:

```python
mix_of_0s_and_1s: Stream[int] = Stream([[0] * 4, [1] * 4]).flatten(concurrency=2)
assert list(mix_of_0s_and_1s) == [0, 1, 0, 1, 0, 1, 0, 1]
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
from requests.exceptions import SSLError

status_codes_ignoring_resolution_errors: Stream[int] = (
    Stream(["https://github.com", "https://foo.bar", "https://github.com/foo/bar"])
    .map(requests.get, concurrency=2)
    .catch(SSLError, when=lambda exception: "Max retries exceeded with url" in str(exception))
    .map(lambda response: response.status_code)
)

assert list(status_codes_ignoring_resolution_errors) == [200, 404]
```

> It has an optional `finally_raise: bool` parameter to raise the first catched exception when iteration ends.

## `.truncate`

> Ends iteration once a given number of elements have been yielded:

```python
five_first_integers: Stream[int] = integers.truncate(5)

assert list(five_first_integers) == [0, 1, 2, 3, 4]
```

> ... or when a condition has become satisfied:

```python
five_first_integers: Stream[int] = integers.truncate(when=lambda n: n == 5)

assert list(five_first_integers) == [0, 1, 2, 3, 4]
```

## `.skip`

> Skips the first specified number of elements:

```python
integers_after_five: Stream[int] = integers.skip(5)

assert list(integers_after_five) == [5, 6, 7, 8, 9]
```

## `.distinct`

> Removes duplicates:

```python
distinct_chars: Stream[str] = Stream("foobarfooo").distinct()

assert list(distinct_chars) == ["f", "o", "b", "a", "r"]
```

> Specify a function to deduplicate based on the value it returns when applied to elements:

```python
strings_of_distinct_lengths: Stream[str] = Stream(["a", "foo", "bar", "z"]).distinct(len)

assert list(strings_of_distinct_lengths) == ["a", "foo"]
```

> [!WARNING]
> During iteration, all distinct elements that are yielded are retained in memory to perform deduplication. However, you can remove only consecutive duplicates without a memory footprint by setting `consecutive_only=True`:

```python
consecutively_distinct_chars: Stream[str] = Stream("foobarfooo").distinct(consecutive_only=True)

assert list(consecutively_distinct_chars) == ["f", "o", "b", "a", "r", "f", "o"]
```

## `.observe`

> Logs the progress of iterations over this stream, if you iterate on:
```python
observed_slow_integers: Stream[int] = slow_integers.observe("integers")
```
> you will get these logs:
```
INFO: [duration=0:00:00.502155 errors=0] 1 integers yielded
INFO: [duration=0:00:01.006336 errors=0] 2 integers yielded
INFO: [duration=0:00:02.011921 errors=0] 4 integers yielded
INFO: [duration=0:00:04.029666 errors=0] 8 integers yielded
INFO: [duration=0:00:05.039571 errors=0] 10 integers yielded
```

> The amount of logs will never be overwhelming because they are produced logarithmically e.g. the 11th log will be produced when the iteration reaches the 1024th element.

> [!WARNING]
> It is mute between *v1.1.0* and *v1.3.1*, please `pip install --upgrade streamable`

## `zip`

> Use the standard `zip` function:

```python
from streamable import star

cubes: Stream[int] = (
    Stream(zip(integers, integers, integers)) # Stream[Tuple[int, int, int]]
    .map(star(lambda a, b, c: a * b * c))
)

assert list(cubes) == [0, 1, 8, 27, 64, 125, 216, 343, 512, 729]
```

---

# 📦 ***Notes Box***
## Contribute
Please help us ! Feel very welcome to:
- [open issues](https://github.com/ebonnal/streamable/issues)
- [open pull requests](https://github.com/ebonnal/streamable/pulls)
- check [CONTRIBUTING.md](CONTRIBUTING.md)


## exhaust the stream

> `.count` iterates over the stream until exhaustion and returns the count of elements yielded.
```python
>>> assert integers.count() == 10
```

> ***calling*** the stream iterates over it until exhaustion and returns it.
```python
>>> verbose_integers: Stream[int] = integers.foreach(print)
>>> assert verbose_integers() is verbose_integers
0
1
2
3
4
5
6
7
8
9
```


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
        # Catches exceptions and raises the 1st one at the end of the iteration
        .catch(finally_raise=True)
        # Actually triggers an iteration (the lines above define lazy operations)
        .count()
    )
```

## logging level
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

## compatible with *free-threaded* Python 3.13+
Benefits from [free-threaded](https://docs.python.org/3/using/configure.html#cmdoption-disable-gil) Python 3.13+ builds, run via `python -X gil=0`.

## Thank you for the highlights 🙏
- [Tryolabs's Top Python libraries of 2024](https://tryolabs.com/blog/top-python-libraries-2024#top-10---general-use) ([LinkedIn](https://www.linkedin.com/posts/tryolabs_top-python-libraries-2024-activity-7273052840984539137-bcGs?utm_source=share&utm_medium=member_desktop), [Reddit](https://www.reddit.com/r/Python/comments/1hbs4t8/the_handpicked_selection_of_the_best_python/))
- [PyCoder’s Weekly](https://pycoders.com/issues/651) x [Real Python](https://realpython.com/)
- [@PythonHub's tweet](https://x.com/PythonHub/status/1842886311369142713)
- [Upvoters on our showcase Reddit post](https://www.reddit.com/r/Python/comments/1fp38jd/streamable_streamlike_manipulation_of_iterables/)
