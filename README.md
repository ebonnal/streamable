# `kioss`
**Keep I/O Simple and Stupid**

[![Actions Status](https://github.com/bonnal-enzo/kioss/workflows/test/badge.svg)](https://github.com/bonnal-enzo/kioss/actions) [![Actions Status](https://github.com/bonnal-enzo/kioss/workflows/PyPI/badge.svg)](https://github.com/bonnal-enzo/kioss/actions)

Expressive `Iterator`-based library that has been designed to **ease the development of (reverse) ETL data pipelines**, with features such as *multithreading*, *rate limiting*, *batching*, and *exceptions handling*.

## Install

`pip install kioss`

## Interface
There is only 1 import:
```python
from kioss import Pipe
``` 
Use this `Pipe` class as follow:
1. instanciate it with a data source
2. plan transformations and controlling operations on it
3. execute it

### Init
- `.__init__` a `Pipe` by providing a `Callable` returning an `Iterator[T]` or `Iterable[T]` as source.

### Plan transformation operations
- `.map` a function over a pipe (optional multithreading).
- `.do` side effects on a pipe by calling an function over it while discarding the results (optional multithreading).
- `.flatten` a pipe whose elements are themselves `Iterator`s (optional multithreading).
- `.filter` a pipe using a predicate function.
- `.chain` several pipes to form a new one that yields elements of one pipe after the previous one is exhausted.
- `.batch` the elements of a pipe and yield them as `list`s of a specific maximum size and/or spanning over a specific period of time.

### Plan controling operations
- `.slow` a pipe, i.e. rate limit the iteration over it.
- `.log` a pipe's iteration advancement (logarithmically, so no spam).
- `.catch` a pipe's exceptions by deciding which specific subtype of `Exception` to catch and whether to ignore it or to yield it.

### Execute the plan
- `.collect` a pipe's elements into a list having an optional max size.
- `.superintend` a pipe, i.e. iterate over it until it is exhausted, with logging and exceptions catching, ultimately returning the collected outputs or logging a sample of the encountered errors and raising if any.

### Inter-operate
The `Pipe[T]` class extends `Iterable[T]`, hence you can pass a pipe to any function supporting iterables:
- `set(pipe)`
- `functools.reduce(func, pipe, initial)`
- `itertools.islice(pipe, n_samples)`
- ...

## Code snippets
### 1. Extract social media messages from GCS and POST the hashtags they contain into a web API
```python
from typing import Iterator
from google.cloud import storage
import requests

from kioss import Pipe


# Let's say we have a bucket
bucket: storage.Bucket = ...
# and an Iterator of object paths
object_paths: Iterator[str] = ...

# Each file contains a message sent on a social network
# and we want to POST their hashtags to an API
(
    # instanciate a Pipe with object paths as data source
    Pipe(source=lambda: object_paths)
    .log(what="object paths")
    # get the blob corresponding to this object path in the given bucket
    .map(bucket.get_blob)
    .log(what="blobs")
    # read the bytes of the blob and decode them to str
    .map(storage.Blob.download_as_string)
    .map(bytes.decode)
    # split the tweet on whitespaces.
    .map(str.split)
    # flatten the pipe to yield individual words
    .map(iter)
    .flatten()
    .log(what="words")
    # keep the word only if it is a hashtag
    .filter(lambda word: word.startswith("#"))
    # remove the '#'
    .map(lambda hashtag: hashtag[1:])
    .log(what="hashtags")
    # send these hashtags to an API endpoint, by batches of 100, using 4 threads,
    # while ensuring that the API will be called at most 30 times per second.
    .batch(size=100)
    .slow(freq=30)
    .map(
        lambda hashtags: requests.post(
            "https://foo.bar",
            headers={'Content-Type': 'application/json'},
            auth=("foo", "bar"),
            json={"hashtags": hashtags},
        ),
        n_workers=4,
    )
    # raise for each response having status code 4XX or 5XX.
    .do(requests.Response.raise_for_status)
    .log(what="hashtags batch integrations")
    # launch the iteration and log its advancement
    # and raise a summary of the raised exceptions if any once the pipe is exhausted.
    .superintend()
)
```
