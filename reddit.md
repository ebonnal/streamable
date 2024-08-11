I am presenting the `streamable` library to you, fellow Python developers, with two classic objectives:

- inform you of its existence
- gather feedback

# What my project does
`streamable` could have been named *"Yet Another Stream-like Library"* (see the Comparison section at the end of this post): a `Stream[T]` inherits from `Iterable[T]` and exposes a fluent interface that allows manipulation of a source iterable by chaining lazy operations, which currently cover:
- grouping/flattening/filtering
- mapping, optionally leveraging threads or asyncio-based concurrency
- catching exceptions
- throttling the rate of iterations over the stream
- ... for more, see the [Operations section in the README](https://github.com/ebonnal/streamable?tab=readme-ov-file#-operations)

# Intro
|||
|--|--|
|🔗 *Fluent*|Chain methods|
|🇹 *Typed*|`Stream[T]` inherits from `Iterable[T]`, library **fully typed**, [`mypy`](https://github.com/python/mypy) it|
|💤 *Lazy*|Operations are **lazily evaluated** at iteration time|
|🔄 *Concurrent*|Seamlessly enjoy **threads** or `asyncio`-based concurrency|
|🛡️ *Robust*|Extensively unittested for **Python 3.7 to 3.12** with 100% coverage|
|🪶 *Light*|`pip install streamable` with **no additional dependencies**|

---


# 1. install

```bash
pip install streamable
```

# 2. import
```python
from streamable import Stream
```

# 3. init
Instantiate a `Stream[T]` from an `Iterable[T]`.

```python
integers: Stream[int] = Stream(range(10))
```

# 4. operate
- `Stream`s are ***immutable***: applying an operation returns a new stream.

- Operations are ***lazy***: only evaluated at iteration time.

```python
inverses: Stream[float] = (
    integers
    .map(lambda n: round(1 / n, 2))
    .catch(ZeroDivisionError)
)
```

# 5. iterate
- Iterate over a `Stream[T]` as you would over any other `Iterable[T]`.
- Source elements are ***processed on-the-fly***.

- collect it:
```python
>>> list(inverses)
[1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
>>> set(inverses)
{0.5, 1.0, 0.2, 0.33, 0.25, 0.17, 0.14, 0.12, 0.11}
```

- reduce it:
```python
>>> sum(integers)
2.82
>>> max(inverses)
1.0
>>> from functools import reduce
>>> reduce(..., inverses)
```

- loop it:
```python
>>> for inverse in inverses:
>>>    ...
```

- next it:
```python
>>> inverses_iter = iter(inverses)
>>> next(inverses_iter)
1.0
>>> next(inverses_iter)
0.5
```

# Target Audience
Even though I hope/guess that it can interest a broader audience, at least as a Data Engineer in a small startup I have found it particularly useful when I had to develop concise Extract-Transform-Load custom scripts.

Here is a toy example (that you can copy-paste and run) that creates a CSV file containing all 67 quadrupeds from the 1st, 2nd, and 3rd generations of Pokémons (kudos to [PokéAPI](https://pokeapi.co/)):
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

# Comparison
A lot of other libraries have filled the same need to chain lazy operations over an iterable (e.g. see [this stackoverflow question](https://stackoverflow.com/questions/24831476/what-is-the-python-way-of-chaining-maps-and-filters/77978940?noredirect=1#comment138494051_77978940)), but the most adopted is [PyFunctional](https://github.com/EntilZha/PyFunctional). For my use case I couldn't use PyFunctional out-of-the-box, I mainly missed:
- full typing (allowing type checking via mypy)
- iteration throttling
- iteration process logging
- exceptions catching

I have decided to create my own library and not to simply propose changes to PyFunctional because I also wanted to take my shot at:
- proposing another fluent interface
- proposing a light approach with no additional dependencies where a `Stream[T]` is just an `Iterable[T]` decorated with chainable lazy operations; the responsabilities of creating the data source and consuming the stream are out of the lib's scope: let's use `from csv import DictWriter` instead of relying on a `stream.to_csv(...)`, or `from functools import reduce` instead of `stream.reduce(...)`
- implementing lazyness using a Visitor Pattern to decouple the declaration of a `Stream[T]` and the construction of an `Iterator[T]` required at iteration time (i.e. happening in the `__iter__` method)

Yes, it is definitely *"Yet Another Stream-like Library"*, let me know if you think that its design efforts and differences with PyFunctional are not enough to justify the release of another library in the nature, and that it makes more sense to propose changes to PyFunctional to cover the few features I missed for my use cases!
