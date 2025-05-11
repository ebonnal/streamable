[![coverage](https://codecov.io/gh/ebonnal/streamable/graph/badge.svg?token=S62T0JQK9N)](https://codecov.io/gh/ebonnal/streamable)
[![testing](https://github.com/ebonnal/streamable/actions/workflows/testing.yml/badge.svg?branch=main)](https://github.com/ebonnal/streamable/actions)
[![typing](https://github.com/ebonnal/streamable/actions/workflows/typing.yml/badge.svg?branch=main)](https://github.com/ebonnal/streamable/actions)
[![formatting](https://github.com/ebonnal/streamable/actions/workflows/formatting.yml/badge.svg?branch=main)](https://github.com/ebonnal/streamable/actions)
[![PyPI](https://github.com/ebonnal/streamable/actions/workflows/pypi.yml/badge.svg?branch=main)](https://pypi.org/project/streamable)
[![Anaconda-Server Badge](https://anaconda.org/conda-forge/streamable/badges/version.svg)](https://anaconda.org/conda-forge/streamable)

# ༄ `streamable`

### *Pythonic Stream-like manipulation of (async) iterables*

- 🔗 ***Fluent*** chainable lazy operations
- 🔀 ***Concurrent*** via *threads*/*processes*/`async`
- 🇹 ***Typed***, fully annotated, `Stream[T]` is both an `Iterable[T]` and an `AsyncIterable[T]`
- 🛡️ ***Tested*** extensively with **Python 3.7 to 3.14**
- 🪶 ***Light***, no dependencies



## 1. install

```bash
pip install streamable
```
*or*
```bash
conda install conda-forge::streamable 
```

## 2. import

```python
from streamable import Stream
```

## 3. init

Create a `Stream[T]` *decorating* an `Iterable[T]` (or an `AsyncIterable[T]`):

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

Iterate over a `Stream[T]` just as you would over any other `Iterable[T]`/`AsyncIterable`, elements are processed *on-the-fly*:


### as `Iterable[T]`
- **into data structure**
```python
>>> list(inverses)
[1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
>>> set(inverses)
{0.5, 1.0, 0.2, 0.33, 0.25, 0.17, 0.14, 0.12, 0.11}
```

- **`for`**
```python
>>> [inverse for inverse in inverses]:
[1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
```

- **`reduce`**
```python
>>> sum(inverses)
2.82
>>> from functools import reduce
>>> reduce(..., inverses)
```

- **`iter`/`next`**
```python
>>> next(iter(inverses))
1.0
```

### as `AsyncIterable[T]`

- **`async for`**
```python
>>> async def main() -> List[int]:
>>>     return [inverse async for inverse in inverses]

>>> asyncio.run(main())
[1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
```

- **`aiter`/`anext`**
```python
>>> asyncio.run(anext(aiter(inverses)))  # before 3.10: inverses.__aiter__().__anext__()
1.0
```



# ↔️ **Extract-Transform-Load**

> [!TIP]
> **ETL scripts** can benefit from the expressiveness of this library. Below is a pipeline that extracts the 67 quadruped Pokémon from the first three generations using [PokéAPI](https://pokeapi.co/) and loads them into a CSV:

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
        # Limits to 16 requests per second to be friendly to our fellow PokéAPI devs
        .throttle(16, per=timedelta(seconds=1))
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
        .catch(Exception, finally_raise=True)
    )

    pipeline()
```

## Or the `async` way

Using `.amap` (the `.map`'s `async` counterpart) and `await`ing the stream (exhausts it as an `AsyncIterable[T]`):

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
                # Limits to 16 requests per second to be friendly to our fellow PokéAPI devs
                .throttle(16, per=timedelta(seconds=1))
                # GETs pokemons via 8 concurrent asyncio coroutines
                .map(lambda poke_id: f"https://pokeapi.co/api/v2/pokemon-species/{poke_id}")
                .amap(http_async_client.get, concurrency=8)
                .foreach(httpx.Response.raise_for_status)
                .map(httpx.Response.json)
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
                .catch(Exception, finally_raise=True)
            )

            await pipeline

asyncio.run(main())
```

# 📒 ***Operations***

*A dozen expressive lazy operations and that’s it!*

## `.map`

> Applies a transformation on elements:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
integer_strings: Stream[str] = integers.map(str)

assert list(integer_strings) == ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
```
</details>

### concurrency

> [!NOTE]
> By default, all the concurrency modes presented below yield results in the upstream order (FIFO). Set the parameter `ordered=False` to yield results as they become available (***First Done, First Out***).

### thread-based concurrency

> Applies the transformation via `concurrency` threads:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

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
> `concurrency` is also the size of the buffer containing not-yet-yielded results. **If the buffer is full, the iteration over the upstream is paused** until a result is yielded from the buffer.

> [!TIP]
> The performance of thread-based concurrency in a CPU-bound script can be drastically improved by using a [Python 3.13+ free-threading build](https://docs.python.org/3/using/configure.html#cmdoption-disable-gil).

### process-based concurrency

> Set `via="process"`:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
if __name__ == "__main__":
    state: List[int] = []
    # integers are mapped
    assert integers.map(state.append, concurrency=4, via="process").count() == 10
    # but the `state` of the main process is not mutated
    assert state == []
```
</details>

### `async`-based concurrency: [see `.amap`](#amap)

> [The `.amap` operation can apply an `async` function concurrently.](#amap)

### "starmap"

> The `star` function decorator transforms a function that takes several positional arguments into a function that takes a tuple:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
from streamable import star

zeros: Stream[int] = (
    Stream(enumerate(integers))
    .map(star(lambda index, integer: index - integer))
)

assert list(zeros) == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
```
</details>



## `.foreach`

> Applies a side effect on elements:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

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
> - [The `.aforeach` operation can apply an `async` effect concurrently.](#aforeach)

## `.group`

> Groups into `List`s

> ... up to a given group `size`:
<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
integers_by_5: Stream[List[int]] = integers.group(size=5)

assert list(integers_by_5) == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]
```
</details>

> ... and/or co-groups `by` a given key:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
integers_by_parity: Stream[List[int]] = integers.group(by=lambda n: n % 2)

assert list(integers_by_parity) == [[0, 2, 4, 6, 8], [1, 3, 5, 7, 9]]
```
</details>

> ... and/or co-groups the elements yielded by the upstream within a given time `interval`:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

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

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
integers_by_parity_by_2: Stream[List[int]] = (
    integers
    .group(by=lambda n: n % 2, size=2)
)

assert list(integers_by_parity_by_2) == [[0, 2], [1, 3], [4, 6], [5, 7], [8], [9]]
```
</details>

## `.groupby`

> Like `.group`, but groups into `(key, elements)` tuples:
<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

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

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
from streamable import star

counts_by_parity: Stream[Tuple[str, int]] = (
    integers_by_parity
    .map(star(lambda parity, ints: (parity, len(ints))))
)

assert list(counts_by_parity) == [("even", 5), ("odd", 5)]
```
</details>

## `.flatten`

> Ungroups elements assuming that they are `Iterable`s:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
even_then_odd_integers: Stream[int] = integers_by_parity.flatten()

assert list(even_then_odd_integers) == [0, 2, 4, 6, 8, 1, 3, 5, 7, 9]
```
</details>

### thread-based concurrency

> Flattens `concurrency` iterables concurrently:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
mixed_ones_and_zeros: Stream[int] = (
    Stream([[0] * 4, [1] * 4])
    .flatten(concurrency=2)
)
assert list(mixed_ones_and_zeros) == [0, 1, 0, 1, 0, 1, 0, 1]
```
</details>

## `.filter`

> Keeps only the elements that satisfy a condition:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
even_integers: Stream[int] = integers.filter(lambda n: n % 2 == 0)

assert list(even_integers) == [0, 2, 4, 6, 8]
```
</details>

## `.distinct`

> Removes duplicates:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
distinct_chars: Stream[str] = Stream("foobarfooo").distinct()

assert list(distinct_chars) == ["f", "o", "b", "a", "r"]
```
</details>

> specifying a deduplication `key`:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

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

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
consecutively_distinct_chars: Stream[str] = (
    Stream("foobarfooo")
    .distinct(consecutive_only=True)
)

assert list(consecutively_distinct_chars) == ["f", "o", "b", "a", "r", "f", "o"]
```
</details>

## `.truncate`

> Ends iteration once a given number of elements have been yielded:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
five_first_integers: Stream[int] = integers.truncate(5)

assert list(five_first_integers) == [0, 1, 2, 3, 4]
```
</details>

> or `when` a condition is satisfied:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
five_first_integers: Stream[int] = integers.truncate(when=lambda n: n == 5)

assert list(five_first_integers) == [0, 1, 2, 3, 4]
```
</details>

> If both `count` and `when` are set, truncation occurs as soon as either condition is met.

## `.skip`

> Skips the first specified number of elements:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
integers_after_five: Stream[int] = integers.skip(5)

assert list(integers_after_five) == [5, 6, 7, 8, 9]
```
</details>

> or skips elements `until` a predicate is satisfied:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
integers_after_five: Stream[int] = integers.skip(until=lambda n: n >= 5)

assert list(integers_after_five) == [5, 6, 7, 8, 9]
```
</details>

> If both `count` and `until` are set, skipping stops as soon as either condition is met.

## `.catch`

> Catches a given type of exception, and optionally yields a `replacement` value:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

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
<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

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
> Apply side effects when catching an exception by integrating them into `when`:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

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

## `.throttle`

> Limits the number of yields `per` time interval:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
from datetime import timedelta

three_integers_per_second: Stream[int] = integers.throttle(3, per=timedelta(seconds=1))

# takes 3s: ceil(10 integers / 3 per_second) - 1
assert list(three_integers_per_second) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```
</details>


## `.observe`

> Logs the progress of iterations:
<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

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
> The amount of logs will never be overwhelming because they are produced logarithmically (base 2): the 11th log will be produced after 1,024 elements have been yielded, the 21th log after 1,048,576 elements, ...

> [!TIP]
> To mute these logs, set the logging level above `INFO`:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
import logging
logging.getLogger("streamable").setLevel(logging.WARNING)
```
</details>

## `+`

> Concatenates streams:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
assert list(integers + integers) == [0, 1, 2, 3 ,4, 5, 6, 7, 8, 9, 0, 1, 2, 3 ,4, 5, 6, 7, 8, 9]
```
</details>


## `zip`

> [!TIP]
> Use the standard `zip` function:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

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
> [!NOTE]
> Although consuming the stream is beyond the scope of this library, it provides two basic shorthands to trigger an iteration:

## `.count`

> Iterates over the stream until exhaustion and returns the number of elements yielded:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
assert integers.count() == 10
```
</details>

## `()`

> *Calling* the stream iterates over it until exhaustion and returns it:
<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
state: List[int] = []
appending_integers: Stream[int] = integers.foreach(state.append)
assert appending_integers() is appending_integers
assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```
</details>

## `.pipe`

> Calls a function, passing the stream as first argument, followed by `*args/**kwargs` if any:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

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

> Inspired by the `.pipe` from [pandas](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.pipe.html) or [polars](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.pipe.html).

---
---

# 📒 ***`async` Operations***

Operations that accept a function as an argument have an `async` counterpart, which has the same signature but accepts `async` functions instead. These `async` operations are named the same as the original ones but with an `a` prefix.

> [!TIP]
> One can mix regular and `async` operations on the same `Stream`, and then consume it as a regular `Iterable` or as an `AsyncIterable`.

## `.amap`

> Applies an `async` transformation on elements:


### Consume as `Iterable[T]`

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
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

assert list(pokemon_names) == ['bulbasaur', 'ivysaur', 'venusaur']
asyncio.run(http_async_client.aclose())
```
</details>

### Consume as `AsyncIterable[T]`

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

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


## `.aforeach`

> Applies an `async` side effect on elements. Supports `concurrency` like `.amap`.

## `.agroup`

> Groups into `List`s according to an `async` grouping function.

## `.agroupby`

> Groups into `(key, elements)` tuples, according to an `async` grouping function.

## `.aflatten`

> Ungroups elements assuming that they are `AsyncIterable`s.

> Like for `.flatten` you can set the `concurrency` parameter.

## `.afilter`

> Keeps only the elements that satisfy an `async` condition.

## `.adistinct`

> Removes duplicates according to an `async` deduplication `key`.

## `.atruncate`

> Ends iteration once a given number of elements have been yielded or `when` an `async` condition is satisfied.

## `.askip`

> Skips the specified number of elements or `until` an `async` predicate is satisfied.

## `.acatch`

> Catches a given type of exception `when` an `async` condition is satisfied.

## Shorthands for consuming the stream as an `AsyncIterable[T]`

## `.acount`

> Iterates over the stream until exhaustion and returns the number of elements yielded:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
assert asyncio.run(integers.acount()) == 10
```
</details>


## `await`

> *Awaiting* the stream iterates over it until exhaustion and returns it:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
async def test_await() -> None:
    state: List[int] = []
    appending_integers: Stream[int] = integers.foreach(state.append)
    appending_integers is await appending_integers
    assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
asyncio.run(test_await())
```
</details>

---
---

# 💡 Notes

## Exceptions are not terminating the iteration

> [!TIP]
> If any of the operations raises an exception, you can resume the iteration after handling it:

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

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

<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

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

## Functions
> [!TIP]
> The `Stream`'s methods are also exposed as functions:
<details ><summary style="text-indent: 40px;">👀 show example</summary></br>

```python
from streamable.functions import catch

inverse_integers: Iterator[int] = map(lambda n: 1 / n, range(10))
safe_inverse_integers: Iterator[int] = catch(inverse_integers, ZeroDivisionError)
```
</details>

# Contributing
**Many thanks to our [contributors](https://github.com/ebonnal/streamable/graphs/contributors)!**

Feel very welcome to help us improve `streamable` via issues and PRs, check [CONTRIBUTING.md](CONTRIBUTING.md).


# 🙏 Community Highlights – Thank You!
- [Tryolabs' Top Python libraries of 2024](https://tryolabs.com/blog/top-python-libraries-2024#top-10---general-use) ([LinkedIn](https://www.linkedin.com/posts/tryolabs_top-python-libraries-2024-activity-7273052840984539137-bcGs?utm_source=share&utm_medium=member_desktop), [Reddit](https://www.reddit.com/r/Python/comments/1hbs4t8/the_handpicked_selection_of_the_best_python/))
- [PyCoder’s Weekly](https://pycoders.com/issues/651) x [Real Python](https://realpython.com/)
- [@PythonHub's tweet](https://x.com/PythonHub/status/1842886311369142713)
- [Upvoters on our showcase Reddit post](https://www.reddit.com/r/Python/comments/1fp38jd/streamable_streamlike_manipulation_of_iterables/)
