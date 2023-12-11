# `kioss`
**Keep I/O Simple and Stupid**

[![Actions Status](https://github.com/bonnal-enzo/kioss/workflows/test/badge.svg)](https://github.com/bonnal-enzo/kioss/actions) [![Actions Status](https://github.com/bonnal-enzo/kioss/workflows/PyPI/badge.svg)](https://github.com/bonnal-enzo/kioss/actions)

Expressive `Iterator`-based library that has been designed to ***ease the development of (reverse) ETL data pipelines***, with features such as *multithreading*, *rate limiting*, *batching*, and *exceptions handling*.

## 1. Install

`pip install kioss`

## 2. Usage
There is only 1 import:
```python
from kioss import Pipe
```

A `Pipe` is an ***immutable*** `Iterable` that you instantiate with a function returning an `Iterable` (the data source).

You can then derive another `Pipe` from it by applying an operation on it.

There are 2 types of operations:

1. ⚙️ transformations
    - `.map` a function over a pipe (optional multithreading).
    - `.filter` a pipe using a predicate function.
    - `.batch` the elements of a pipe and return them as `List`s of a specific maximum size and/or spanning over a specific period of time.
    - `.flatten` a pipe whose elements are themselves `Iterator`s (optional multithreading).
    - `.chain` several pipes to form a new one that returns elements of one pipe after the previous one is exhausted.
    - `.do` side effects on a pipe: Like `.map` but returns the input instead of the function's result (optional multithreading).

2. 🎛️ controls
    - `.slow` a pipe to a given frequency (i.e. rate limit the iteration over it).
    - `.log` a pipe's iteration advancement (logarithmically, so no spam).
    - `.catch` pipe's `Exception`s of a given type and decide whether to ignore them or to return them.

Applying an operation on a pipe just returns another pipe, but ***to execute it you have to iterate over it***:
- `for elem in pipe`
- `functools.reduce(func, pipe, initial)`
- `set(pipe)`

Alternatively you can use one of these methods:
- `.collect` a pipe's elements into a list having an optional max size.
- `.superintend` a pipe:
    - iterates over it until it is exhausted,
    - logs
    - catches exceptions and if any raises a sample of them at the end of the iteration
    - returns a sample of the output elements


## 3. Example
### Christmas comments translation and integration
```python
import logging
from operator import itemgetter

import requests
from google.cloud import bigquery, translate
from kioss import Pipe

# Define your pipeline's plan:
christmas_comments_integration_pipe: Pipe[str] = (
    # Read the comments made on your platform from your BigQuery datawarehouse
    Pipe(bigquery.Client().query("SELECT text FROM fact.comment").result)
    .map(itemgetter("text"))
    .log(what="comments")

    # translate them in english concurrently using 4 threads
    # by batch of 20 at a maximum rate of 50 batches per second
    .batch(size=20)
    .slow(freq=50)
    .map(translate.Client("en").translate, n_threads=4)
    .flatten()
    .map(itemgetter("translatedText"))
    .log(what="comments translated in english")

    # keep only the ones containing christmas
    .filter(lambda comment: "christmas" in comment.lower())
    .log(what="christmas comments")

    # POST these comments in some endpoint using 2 threads
    # by batch of maximum size 100 and with at least 1 batch every 10 seconds
    # and at a maximum rate of 5 batches per second.
    # Also raise if the status code is not 200.
    .batch(size=100, period=10)
    .slow(freq=5)
    .map(
        lambda comment: requests.post(
            "https://some.endpoint/comment",
            json={"text": comment},
            auth=("foo", "bar"),
        ),
        n_threads=2,
    )
    .do(requests.Response.raise_for_status)
    .map(requests.Response.text)
    .log(what="integration response's texts")
)

# At this point you have just defined a pipeline plan but nothing else happened.
# You can now execute the pipe by iterating over it like you whish,
# but we will here use the handy `superintend` method that:
# - iterates over the pipe until it is exhausted
# - catches errors along the way and if any raises them after the iteration
# - returns a sample of output elements

logging.info(
    "Some samples of the integration's responses: %s",
    christmas_comments_integration_pipe.superintend(n_samples=5)
)
```
