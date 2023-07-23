# `kioss`
**Keep I/O Simple and Stupid**

[![Actions Status](https://github.com/bonnal-enzo/kioss/workflows/test/badge.svg)](https://github.com/bonnal-enzo/kioss/actions) [![Actions Status](https://github.com/bonnal-enzo/kioss/workflows/PyPI/badge.svg)](https://github.com/bonnal-enzo/kioss/actions)

## Install

`python -m pip install kioss`

## Overview

```python
from kioss import Pipe

words_count: int = (
    Pipe(open("...", "r"))
    .map(str.split)
    .explode()
    .map(lambda _: 1)
    .reduce(int.__add__, initial=0)
)
```

## Features
- mutate
    - `.merge` several pipes to form a new one that yields elements using multiple threads
    - `.chain` several pipes to form a new one that yields elements of one pipe after the previous one is exhausted.
    - `.map` over pipe's elements uing multiple threads
    - `.flatten` a pipeline that yields iterable elements to make it separately yield each object contained in the element
    - `.filter` a pipe
    - `.batch` pipe's elements and yield them as lists of a given max size or spanning over a given max period.
- manage
    - `.log` a pipe's iteration status
    - `.catch` any pipe's error to treat it after iteration
    - `.slow` a pipe to limit the iteration's speed over it
- consume
    - `.collect` a pipe into a list having an optional max size
    - `.reduce` a pipe to a result using a binary function
    - `.superintend` a pipe: iterate over it entirely while catching exceptions + logging the iteration process + collecting and raising error samples.


## Setup

```bash
python -m venv .venv
source ./.venv/bin/activate
python -m unittest
python -m black kioss/* test/* 
```
