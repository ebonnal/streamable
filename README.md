# `kioss`
**Keep I/O Simple and Stupid**

[![Actions Status](https://github.com/bonnal-enzo/kioss/workflows/test/badge.svg)](https://github.com/bonnal-enzo/kioss/actions) [![Actions Status](https://github.com/bonnal-enzo/kioss/workflows/PyPI/badge.svg)](https://github.com/bonnal-enzo/kioss/actions)

Write concise and expressive definitions of ETL and Reverse ETL pipelines. This library has been specifically designed to be convenient for handling data integration from and to APIs, with features such as multithreading, rate limiting, batching, and exception handling.

## Install

`pip install kioss`

## Toy use case:

```python
import re
from kioss import Pipe
import requests

# detects emails domains in text and raises if any of them is unreachable


# A Pipe can be created from any iterator/iterable,
# it is designed to easily process on the fly the data elements
# from large files, APIs or databases without requiring large memory or disk.
# we use a local file for this example:
with open("/path/to/file.text", "r") as text_file:
    if unreachable_domain_samples := (
        # this pipe will read the text file line by line
        Pipe(text_file)
        # split each map on spaces to get iterator on words
        .map(str.split)
        .map(iter)
        # flatten the pipe to make it yield individual words
        .flatten()
        # log the advancement of this step
        .log(what="parsed words")

        # parse the word to get the email domain in it if any
        .map(lambda word: re.search(r"@([a-zA-Z0-9.-]+)", word).group(1))
        # catch exception produced by non-email words and ignore them
        .catch(AttributeError, ignore=True)
        # log the advancement of this step
        .log(what="parsed email domains")

        # batch the words into chucks of 500 words at most and not spanning over more than a 1 minute
        .batch(size=500, secs=60)
        # deduplicate the email domains inside the batch
        .map(set)
        .map(iter)
        # flatten back to yield individual domains from a batch
        .flatten()
        # log the advancement of this step
        .log(what="parsed email domains deduplicated by batch")

        # construct url from email domain
        .map(lambda email_domain: f"https://{email_domain}")
        # sent GET requests to urls, using 2 threads for better I/O
        .map(requests.get, n_workers=2)
        # limit requests to roughly 20 requests sent by second to avoid spam
        .slow(freq=20)
        # catch request errors without ignoring them this time:
        # log the advancement of this step
        .log(what="requests to domain")

        # it means that the pipeline will yield the exception object encountered instead of raising it
        .catch(requests.RequestException, ignore=False)
        # get only errors, i.e. non-200 status codes or request exceptions (yielded by upstream because ignore=False)
        .filter(lambda reponse: isinstance(reponse, requests.RequestException) or reponse.status_code != 200)
        # iterate over the entire pipe but only store the 32 first errors
        .collect(n_samples=32) 
    ):
        raise RuntimeError(f"Encountered unreachable domains, samples: {unreachable_domain_samples}")
```

## Features
- define:
    - The `.__init__` of the `Pipe` class takes as argument an instance of `Iterator[T]` or `Iterable[T]` used as the source of elements.
    - `.map` a function over a pipe, optionally using multiple threads or processes.
    - `.flatten` a pipe, whose elements are assumed to be iterators, creating a new pipe with individual elements.
    - `.filter` a pipe using a predicate function.
    - `.do` side effects on a pipe, optionally using multiple threads or processes.
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
-----
Note that the `Pipe` class itself extends `Iterator[T]`, hence you can pass a pipe to any function supporting iterators:
- `set(pipe)`
- `functools.reduce(func, pipe, initial)`
- `itertools.islice(pipe, n_samples)`
- ...
  
