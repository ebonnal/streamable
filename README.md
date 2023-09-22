# `kioss`
**Keep I/O Simple and Stupid**

[![Actions Status](https://github.com/bonnal-enzo/kioss/workflows/test/badge.svg)](https://github.com/bonnal-enzo/kioss/actions) [![Actions Status](https://github.com/bonnal-enzo/kioss/workflows/PyPI/badge.svg)](https://github.com/bonnal-enzo/kioss/actions)

Write concise and expressive definitions of ETL and Reverse ETL pipelines. This library has been specifically designed to be convenient for handling data integration from and to APIs, with features such as multithreading, rate limiting, batching, and exception handling.

## Install

`pip install kioss`

## Example

```python
from typing import Iterator
from google.cloud import storage
import requests

from kioss.pipe import Pipe


# Let's say we have a bucket
bucket: storage.Bucket = ...
# and an Iterator of object paths
object_paths: Iterator[str] = ...

# Each file contains a message sent on a social network
# and we want to POST their hashtags to an API
(
    # instanciate a Pipe with object paths as data source
    Pipe(object_paths)
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

## Features
- define:
    - The `.__init__` of the `Pipe` class takes as argument an instance of `Iterator[T]` or `Iterable[T]` used as the source of elements.
    - `.map` a function over a pipe, optionally using multiple threads or processes.
    - `.flatten` a pipe, whose elements are assumed to be iterators, creating a new pipe with individual elements.
    - `.filter` a pipe using a predicate function.
    - `.do` side effects on a pipe, i.e. apply a function ignoring its returned value, optionally using multiple threads or processes.
    - `.chain` several pipes to form a new one that yields elements of one pipe after the previous one is exhausted.
    - `.mix` several pipes to form a new one that yields elements concurrently as they arrive, using multiple threads.
    - `.batch` pipe's elements and yield them as lists of a given size or spanning over a given duration.
- control:
    - `.slow` a pipe to limit the rate of the iteration over it.
    - `.log` a pipe to get an idea of the status of the iteration.
    - `.catch` pipe's exceptions.
- consume:
    - `.collect` a pipe into a list having an optional max size.
    - `.superintend` a pipe: iterate over it entirely while catching exceptions + logging the iteration process + collecting and raising error samples.
------
Note that the `Pipe` class itself extends `Iterator[T]`, hence you can pass an instance to any function supporting iterators:
- `set(pipe)`
- `functools.reduce(func, pipe, initial)`
- `itertools.islice(pipe, n_samples)`
- ...
  
