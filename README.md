[![coverage](https://codecov.io/gh/ebonnal/streamable/graph/badge.svg?token=S62T0JQK9N)](https://codecov.io/gh/ebonnal/streamable)
[![PyPI](https://github.com/ebonnal/streamable/actions/workflows/pypi.yml/badge.svg?branch=main)](https://pypi.org/project/streamable)
[![Anaconda-Server Badge](https://anaconda.org/conda-forge/streamable/badges/version.svg?)](https://anaconda.org/conda-forge/streamable)

# ༄ `streamable`

### *concurrent & fluent interface for (async) iterables*

- 🔗 chain ***lazy*** operations
- 🔀 ***concurrent*** via threads / processes / coroutines
- 🇹 ***typed***, `Stream[T]` is an `Iterable[T]` and an `AsyncIterable[T]`
- 🛡️ ***battle-tested*** for prod, extensively tested with CPython 3.7 to 3.15 and compatible with [PyPy](https://github.com/pypy/pypy).


## 1. install

(No dependencies)
```
pip install streamable
```

## 2. import

```python
from streamable import Stream
```

## 3. init

Create a `Stream[T]` ***decorating*** an `Iterable[T]` (or an `AsyncIterable[T]`):

```python
integers: Stream[int] = Stream(range(10))
```

## 4. operate

Chain ***lazy*** operations (only evaluated during iteration), each returning a new ***immutable*** `Stream`:

```python
inverses: Stream[float] = (
    integers
    .map(lambda n: round(1 / n, 2))
    .catch(ZeroDivisionError)
)
```

## 5. iterate

Iterate over a `Stream[T]` just as you would over any other `Iterable[T]` (or `AsyncIterable[T]`), elements are processed ***on-the-fly***.

- consume as an `Iterable[T]`:
  - `list(inverses)`
  - `set(inverses)`
  - `[inverse for inverse in inverses]`
  - `functools.reduce(operator.mul, inverses)`
  - `next(iter(inverses))`

- consume as an `AsyncIterable[T]` (makes sense when async operations like `.amap` are involved):
  - `[inverse async for inverse in inverses]`
  - `anext(aiter(inverses))`


# ↔ Showcase: Extract-Transform-Load

Let's take an example showcasing most of the `Stream`'s operations.

This script fetches Pokémons from [PokéAPI](https://pokeapi.co/) and writes the quadruped ones from the first three generations by batch into a CSV:

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

    pipeline: Stream = (
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

    # Start a full iteration
    pipeline()
```

## ... or the `async` way

- instead of fetching Pokemons concurrently via threads using `requests` and the `.map` operation, let's **fetch them concurrently via coroutines** using `httpx` and the `.amap` operation
- instead of calling `pipeline()` to iterate over it as an `Iterable`, let's `await pipeline` to iterate over it as an `AsyncIterable`

```python
import asyncio
import csv
from datetime import timedelta
import itertools
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

            # Start a full async iteration
            await pipeline

asyncio.run(main())
```

# 💡 Notes

## Performances

Creating a `Stream` is lazy and fast,

```python
stream: Stream[int] = (
    Stream(range(1_000_000))
    .map(str)
    .map(int)
    .filter(lambda i: i > 1_000)
)
```

and there is **zero overhead during iteration compared to builtins** because `iter(stream)` visits the operations lineage and simply returns:

```python
filter(lambda i: i > 1_000, map(int, map(str, range(1_000_000))))
```

## Exceptions are not terminating the iteration

> [!TIP]
> If any of the operations raises an exception, you can resume the iteration after handling it:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
from contextlib import suppress

casted_ints: Iterator[int] = iter(
    Stream("0123_56789")
    .map(int)
    .group(3)
    .flatten()
)
collected: List[int] = []

with suppress(ValueError):
    collected.extend(casted_ints)
assert collected == [0, 1, 2, 3]

collected.extend(casted_ints)
assert collected == [0, 1, 2, 3, 5, 6, 7, 8, 9]
```

</details >

## Visitor Pattern
> [!TIP]
> A `Stream` can be visited via its `.accept` method: implement a custom [***visitor***](https://en.wikipedia.org/wiki/Visitor_pattern) by extending the abstract class `streamable.visitors.Visitor`:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
from streamable.visitors import Visitor

class DepthVisitor(Visitor[int]):
    def visit_stream(self, stream: Stream) -> int:
        if not stream.upstream:
            return 1
        return 1 + stream.upstream.accept(self)

def depth(stream: Stream) -> int:
    return stream.accept(DepthVisitor())

assert depth(Stream(range(10)).map(str).foreach(print)) == 3
```
</details>

## Contributing
**Many thanks to our [contributors](https://github.com/ebonnal/streamable/graphs/contributors)!**

Feel very welcome to help us improve `streamable` via issues and PRs, check [CONTRIBUTING.md](CONTRIBUTING.md).


## 🙏 Community Highlights – Thank You!
- [Tryolabs' Top 10 Python libraries of 2024](https://tryolabs.com/blog/top-python-libraries-2024#top-10---general-use) ([LinkedIn](https://www.linkedin.com/posts/tryolabs_top-python-libraries-2024-activity-7273052840984539137-bcGs?utm_source=share&utm_medium=member_desktop), [Reddit](https://www.reddit.com/r/Python/comments/1hbs4t8/the_handpicked_selection_of_the_best_python/))
- [PyCoder’s Weekly](https://pycoders.com/issues/651) x [Real Python](https://realpython.com/)
- [@PythonHub's tweet](https://x.com/PythonHub/status/1842886311369142713)
- [Upvoters on our showcase Reddit post](https://www.reddit.com/r/Python/comments/1fp38jd/streamable_streamlike_manipulation_of_iterables/)


# 📒 ***Operations***

> [!IMPORTANT]
> A `Stream` exposes a minimalist interface to manipulate elements, creating its source or consuming it is not its responsability (combine it with dedicated libraries like `functools`, `csv`, `json`, `pyarrow`, `psycopg2`, `boto3`, `requests`, ...).

> [!NOTE]
> **`async` twin operations:** For each operation that takes a function (e.g. `.map`) there is also a version that accepts an `async` function (e.g. `.amap`). You can mix both types of operations on the same `Stream`, which can then be used as either an `Iterable` or an `AsyncIterable`.

## 🟡 `.map`/`.amap`

> Applies a transformation on elements:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
integer_strings: Stream[str] = integers.map(str)

assert list(integer_strings) == ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
```
</details>

### thread-based concurrency

> Applies the transformation via `concurrency` threads.

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

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
</details>

> [!NOTE]
> **Memory-efficient**: Only `concurrency` upstream elements are pulled for processing; the next upstream element is pulled only when a result is yielded downstream.

> [!NOTE]
> **Ordering**: it yields results in the upstream order (FIFO), set `ordered=False` to yield results as they become available (*First Done, First Out*).

### process-based concurrency

> Same but set `via="process"`:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
if __name__ == "__main__":
    state: List[int] = []
    # integers are mapped
    assert integers.map(state.append, concurrency=4, via="process").count() == 10
    # but the `state` of the main process is not mutated
    assert state == []
```
</details>

### coroutine-based concurrency: `.amap`

> `.amap` can apply an `async` function concurrently.

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

- consumed as an `Iterable[T]`:

```python
import httpx

pokemon_names: Stream[str] = (
    Stream(range(1, 4))
    .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
    .amap(httpx.AsyncClient().get, concurrency=3)
    .map(httpx.Response.json)
    .map(lambda poke: poke["name"])
)

assert list(pokemon_names) == ['bulbasaur', 'ivysaur', 'venusaur']
```

- consumed as an `AsyncIterable[T]`:

```python
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
```
</details>

### "starmap"

> The `star` function decorator transforms a function that takes several positional arguments into a function that takes a tuple:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
from streamable import star

zeros: Stream[int] = (
    Stream(enumerate(integers))
    .map(star(lambda index, integer: index - integer))
)

assert list(zeros) == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
```
</details>


## 🟡 `.foreach` / `.aforeach`

> Applies a side effect on elements:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
state: List[int] = []
appending_integers: Stream[int] = integers.foreach(state.append)

assert list(appending_integers) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```
</details>

### concurrency

> Similar to `.map`:
> - set the `concurrency` parameter for **thread-based concurrency**
> - set `via="process"` for **process-based concurrency**
> - set `ordered=False` for ***First Done First Out***
> - The `.aforeach` operation can apply an `async` effect concurrently.

## 🟡 `.group` / `.agroup`

> Groups into `List`s

> ... up to a given group `size`:
<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
integers_by_5: Stream[List[int]] = integers.group(size=5)

assert list(integers_by_5) == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]
```
</details>

> ... and/or co-groups `by` a given key:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
integers_by_parity: Stream[List[int]] = integers.group(by=lambda n: n % 2)

assert list(integers_by_parity) == [[0, 2, 4, 6, 8], [1, 3, 5, 7, 9]]
```
</details>

> ... and/or co-groups the elements yielded by the upstream within a given time `interval`:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
from datetime import timedelta

integers_within_1_sec: Stream[List[int]] = (
    integers
    .throttle(2, per=timedelta(seconds=1))
    .group(interval=timedelta(seconds=0.99))
)

assert list(integers_within_1_sec) == [[0, 1, 2], [3, 4], [5, 6], [7, 8], [9]]
```
</details>

> [!TIP]
> Combine the `size`/`by`/`interval` parameters:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
integers_by_parity_by_2: Stream[List[int]] = (
    integers
    .group(by=lambda n: n % 2, size=2)
)

assert list(integers_by_parity_by_2) == [[0, 2], [1, 3], [4, 6], [5, 7], [8], [9]]
```
</details>

## 🟡 `.groupby` / `.agroupby`

> Like `.group`, but groups into `(key, elements)` tuples:
<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
integers_by_parity: Stream[Tuple[str, List[int]]] = (
    integers
    .groupby(lambda n: "odd" if n % 2 else "even")
)

assert list(integers_by_parity) == [("even", [0, 2, 4, 6, 8]), ("odd", [1, 3, 5, 7, 9])]
```
</details>

> [!TIP]
> Then *"starmap"* over the tuples:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
from streamable import star

counts_by_parity: Stream[Tuple[str, int]] = (
    integers_by_parity
    .map(star(lambda parity, ints: (parity, len(ints))))
)

assert list(counts_by_parity) == [("even", 5), ("odd", 5)]
```
</details>

## 🟡 `.flatten` / `.aflatten`

> Ungroups elements assuming that they are `Iterable`s (or `AsyncIterable`s for `.aflatten`):

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
even_then_odd_integers: Stream[int] = integers_by_parity.flatten()

assert list(even_then_odd_integers) == [0, 2, 4, 6, 8, 1, 3, 5, 7, 9]
```
</details>

### thread-based concurrency

> Concurrently flattens `concurrency` iterables via threads (or via coroutines for `.aflatten`):

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
mixed_ones_and_zeros: Stream[int] = (
    Stream([[0] * 4, [1] * 4])
    .flatten(concurrency=2)
)
assert list(mixed_ones_and_zeros) == [0, 1, 0, 1, 0, 1, 0, 1]
```
</details>

## 🟡 `.filter` / `.afilter`

> Keeps only the elements that satisfy a condition:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
even_integers: Stream[int] = integers.filter(lambda n: n % 2 == 0)

assert list(even_integers) == [0, 2, 4, 6, 8]
```
</details>

## 🟡 `.distinct` / `.adistinct`

> Removes duplicates:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
distinct_chars: Stream[str] = Stream("foobarfooo").distinct()

assert list(distinct_chars) == ["f", "o", "b", "a", "r"]
```
</details>

> specifying a deduplication `key`:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
strings_of_distinct_lengths: Stream[str] = (
    Stream(["a", "foo", "bar", "z"])
    .distinct(len)
)

assert list(strings_of_distinct_lengths) == ["a", "foo"]
```
</details>

> [!WARNING]
> During iteration, all distinct elements that are yielded are retained in memory to perform deduplication. However, you can remove only consecutive duplicates without a memory footprint by setting `consecutive_only=True`:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
consecutively_distinct_chars: Stream[str] = (
    Stream("foobarfooo")
    .distinct(consecutive_only=True)
)

assert list(consecutively_distinct_chars) == ["f", "o", "b", "a", "r", "f", "o"]
```
</details>

## 🟡 `.truncate` / `.atruncate`

> Ends iteration once a given number of elements have been yielded:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
five_first_integers: Stream[int] = integers.truncate(5)

assert list(five_first_integers) == [0, 1, 2, 3, 4]
```
</details>

> or `when` a condition is satisfied:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
five_first_integers: Stream[int] = integers.truncate(when=lambda n: n == 5)

assert list(five_first_integers) == [0, 1, 2, 3, 4]
```
</details>

> If both `count` and `when` are set, truncation occurs as soon as either condition is met.

## 🟡 `.skip` / `.askip`

> Skips the first specified number of elements:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
integers_after_five: Stream[int] = integers.skip(5)

assert list(integers_after_five) == [5, 6, 7, 8, 9]
```
</details>

> or skips elements `until` a predicate is satisfied:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
integers_after_five: Stream[int] = integers.skip(until=lambda n: n >= 5)

assert list(integers_after_five) == [5, 6, 7, 8, 9]
```
</details>

> If both `count` and `until` are set, skipping stops as soon as either condition is met.

## 🟡 `.catch` / `.acatch`

> Catches a given type of exception, and optionally yields a `replacement` value:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
inverses: Stream[float] = (
    integers
    .map(lambda n: round(1 / n, 2))
    .catch(ZeroDivisionError, replacement=float("inf"))
)

assert list(inverses) == [float("inf"), 1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
```
</details>

> You can specify an additional `when` condition for the catch:
<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
import requests
from requests.exceptions import ConnectionError

status_codes_ignoring_resolution_errors: Stream[int] = (
    Stream(["https://github.com", "https://foo.bar", "https://github.com/foo/bar"])
    .map(requests.get, concurrency=2)
    .catch(ConnectionError, when=lambda error: "Max retries exceeded with url" in str(error))
    .map(lambda response: response.status_code)
)

assert list(status_codes_ignoring_resolution_errors) == [200, 404]
```
</details>

> It has an optional `finally_raise: bool` parameter to raise the first exception caught (if any) when the iteration terminates.

> [!TIP]
> Leverage `when` to apply side effects on catch:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
errors: List[Exception] = []

def store_error(error: Exception) -> bool:
    errors.append(error)  # applies effect
    return True  # signals to catch the error

integers_in_string: Stream[int] = (
    Stream("012345foo6789")
    .map(int)
    .catch(ValueError, when=store_error)
)

assert list(integers_in_string) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
assert len(errors) == len("foo")
```
</details>

## 🟡 `.throttle`

> Limits the number of yields `per` time interval:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
from datetime import timedelta

three_integers_per_second: Stream[int] = integers.throttle(3, per=timedelta(seconds=1))

# takes 3s: ceil(10 integers / 3 per_second) - 1
assert list(three_integers_per_second) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```
</details>


## 🟡 `.observe`

> Logs the progress of iterations:
<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
>>> assert list(integers.throttle(2, per=timedelta(seconds=1)).observe("integers")) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

```
INFO: [duration=0:00:00.001793 errors=0] 1 integers yielded
INFO: [duration=0:00:00.004388 errors=0] 2 integers yielded
INFO: [duration=0:00:01.003655 errors=0] 4 integers yielded
INFO: [duration=0:00:03.003196 errors=0] 8 integers yielded
INFO: [duration=0:00:04.003852 errors=0] 10 integers yielded
```
</details>

> [!NOTE]
> To avoid flooding, logs are emitted only when the number of yielded elements (or errors) reaches powers of 2.


> [!TIP]
> To mute these logs, set the logging level above `INFO`:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
import logging
logging.getLogger("streamable").setLevel(logging.WARNING)
```
</details>

## 🟡 `+`

> Concatenates streams:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
assert list(integers + integers) == [0, 1, 2, 3 ,4, 5, 6, 7, 8, 9, 0, 1, 2, 3 ,4, 5, 6, 7, 8, 9]
```
</details>


##  🟡 `zip`

> Use the standard `zip` function:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
from streamable import star

cubes: Stream[int] = (
    Stream(zip(integers, integers, integers))  # Stream[Tuple[int, int, int]]
    .map(star(lambda a, b, c: a * b * c))  # Stream[int]
)

assert list(cubes) == [0, 1, 8, 27, 64, 125, 216, 343, 512, 729]
```
</details>


## Shorthands for consuming the stream

Although consuming the stream is beyond the scope of this library, it provides two basic shorthands to trigger an iteration:

## 🟡 `.count` / `.acount`

> `.count` iterates over the stream until exhaustion and returns the number of elements yielded:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
assert integers.count() == 10
```
</details>

> The `.acount` (`async` method) iterates over the stream as an `AsyncIterable` until exhaustion and returns the number of elements yielded:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
assert asyncio.run(integers.acount()) == 10
```

</details>

## 🟡 `()` / `await`

> *Calling* the stream iterates over it until exhaustion, and returns it:
<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
state: List[int] = []
appending_integers: Stream[int] = integers.foreach(state.append)
assert appending_integers() is appending_integers
assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```
</details>

> *Awaiting* the stream iterates over it as an `AsyncIterable` until exhaustion, and returns it:

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
async def test_await() -> None:
    state: List[int] = []
    appending_integers: Stream[int] = integers.foreach(state.append)
    appending_integers is await appending_integers
    assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
asyncio.run(test_await())
```
</details>

## 🟡 `.pipe`

> Calls a function, passing the stream as first argument, followed by `*args/**kwargs` if any (inspired by the `.pipe` from [pandas](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.pipe.html) or [polars](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.pipe.html)):

<details ><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
import pandas as pd

(
    integers
    .observe("ints")
    .pipe(pd.DataFrame, columns=["integer"])
    .to_csv("integers.csv", index=False)
)
```
</details>
