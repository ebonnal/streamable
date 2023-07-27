# `kioss`
**Keep I/O Simple and Stupid**

[![Actions Status](https://github.com/bonnal-enzo/kioss/workflows/test/badge.svg)](https://github.com/bonnal-enzo/kioss/actions) [![Actions Status](https://github.com/bonnal-enzo/kioss/workflows/PyPI/badge.svg)](https://github.com/bonnal-enzo/kioss/actions)

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
        .log(objects_description="parsed words")

        # parse the word to get the email domain in it if any
        .map(lambda word: re.search(r"@([a-zA-Z0-9.-]+)", word).group(1))
        # catch exception produced by non-email words and ignore them
        .catch(AttributeError, ignore=True)
        # log the advancement of this step
        .log(objects_description="parsed email domains")

        # batch the words into chucks of 500 words at most and not spanning over more than a 1 minute
        .batch(max_size=500, time_window_seconds=60)
        # deduplicate the email domains inside the batch
        .map(set)
        .map(iter)
        # flatten back to yield individual domains from a batch
        .flatten()
        # log the advancement of this step
        .log(objects_description="parsed email domains deduplicated by batch")

        # construct url from email domain
        .map(lambda email_domain: f"https://{email_domain}")
        # sent GET requests to urls, using 2 threads for better I/O
        .map(requests.get, n_threads=2)
        # limit requests to roughly 20 requests sent by second to avoid spam
        .slow(freq=20)
        # catch request errors without ignoring them this time:
        # it means that the pipeline will yield the exception object encountered instead of raising it
        .catch(requests.RequestException, ignore=False)
        # log the advancement of this step
        .log(objects_description="domain responses")

        # get only errors, i.e. non-200 status codes or request exceptions (yielded by upstream because ignore=False)
        .filter(lambda reponse: isinstance(reponse, requests.RequestException) or reponse.status_code != 200)
        # iterate over the entire pipe but only store the 32 first errors
        .collect(limit=32) 
    ):
        raise RuntimeError(f"Encountered unreachable domains, samples: {unreachable_domain_samples}")
```

## Features
- mutate
    - `.mix` several pipes to form a new one that yields elements concurrently as they arrive, using multiple threads.
    - `.chain` several pipes to form a new one that yields elements of one pipe after the previous one is exhausted.
    - `.map` over pipe's elements and yield the results as they arrive, using multiple threads.
    - `.flatten` a pipe, whose elements are assumed to be iterators, creating a new pipe with individual elements.
    - `.filter` a pipe.
    - `.batch` pipe's elements and yield them as lists of a given max size or spanning over a given max period.
- control
    - `.log` a pipe's iteration status.
    - `.catch` a pipe's exceptions of a specific class and return them instead of raising.
    - `.slow` a pipe to limit the iteration's speed over it.
- consume
    - `.collect` a pipe into a list having an optional max size.
    - `.reduce` a pipe to a result using a binary function.
    - `.superintend` a pipe: iterate over it entirely while catching exceptions + logging the iteration process + collecting and raising error samples.
  
