# `streamable`: *elegant iteration*

[![Actions Status](https://github.com/ebonnal/streamable/workflows/unittest/badge.svg)](https://github.com/ebonnal/streamable/actions)
[![Actions Status](https://github.com/ebonnal/streamable/workflows/typing/badge.svg)](https://github.com/ebonnal/streamable/actions)
[![Actions Status](https://github.com/ebonnal/streamable/workflows/lint/badge.svg)](https://github.com/ebonnal/streamable/actions)
[![Actions Status](https://github.com/ebonnal/streamable/workflows/PyPI/badge.svg)](https://github.com/ebonnal/streamable/actions)

## 1. install

```bash
pip install streamable
```

## 2. import
```python
from streamable import Stream
```

## 3. init

```python
integers: Stream[int] = Stream(lambda: range(10))
```

Instantiate a `Stream` by providing a function that returns a fresh `Iterable` (the data source).

## 4. operate

Applying an operation:
- is ***lazy*** i.e it does not iterate over the source
- returns a ***child*** stream without modifying the parent

```python
odd_square_strings: Stream[str] = (
    integers
    .map(lambda x: x ** 2)
    .filter(lambda x: x % 2 == 1)
    .map(str)
)
```

## 5. iterate
Because `Stream[T]` extends `Iterable[T]` you can:
```python
set(odd_squares)
```
```python
sum(odd_squares)
```
```python
for i in odd_squares:
    ...
```

---

# 📒 ***Operations***

## `.map`
Defines the application of a function on parent elements.
```python
integer_strings: Stream[str] = integers.map(str)
```

It has an optional `concurrency` parameter to run the function concurrently (threads).

## `.do`
Defines the application of a function on parent elements like `.map`, but the parent elements are forwarded instead of the result of the function.

```python
printed_integers: Stream[int] = integers.do(print)
```

It has an optional `concurrency` parameter to run the function concurrently (threads).

## `.filter`
Defines the filtering of parent elements based on a predicate function.

```python
pair_integers: Stream[int] = integers.filter(lambda x: x % 2 == 0)
```

## `.batch`

Defines the grouping of parent elements into batches.

```python
integer_batches: Stream[List[int]] = integers.batch(size=100, seconds=60)
```

Here a batch is a list of 100 elements but it may contain less elements in these cases:
- upstream is exhausted
- an exception occurred upstream
- more than `seconds` have elapsed since the last batch.

## `.flatten`

Defines the ungrouping of parent elements assuming that they are `Iterable`s.

```python
integers: Stream[int] = integer_batches.flatten()
```

It has an optional `concurrency` parameter to flatten several parent iterables concurrently (threads).

## `.slow`

Defines a maximum rate at which parent elements are yielded.

```python
slowed_integers: Stream[int] = integers.slow(frequency=2)
```

The `frequency` is expressed in elements per second.

## `.catch`

Defines the catching of the provided exception types.

```python
inverse_floats: Stream[float] = integers.map(lambda x: 1 / x)
safe_inverse_floats: Stream[float] = inverse_floats.catch(ZeroDivisionError)
```

It has optional parameters:
- `when`: a predicate function to decide if the exception has to be catched.
- `raise_at_exhaustion`: to raise the first catched exception at upstream's exhaustion.

## `.observe`

Defines the logging of the evolution of any iteration over the stream.

With
```python
observed_slowed_integers: Stream[int] = slowed_integers.observe(what="integers from 0 to 9")
```

you should get:

```
INFO: iteration over 'integers from 0 to 9' will be observed.
INFO: after 0:00:00.000283, 0 error and 1 `integers from 0 to 9` yielded.
INFO: after 0:00:00.501373, 0 error and 2 `integers from 0 to 9` yielded.
INFO: after 0:00:01.501346, 0 error and 4 `integers from 0 to 9` yielded.
INFO: after 0:00:03.500864, 0 error and 8 `integers from 0 to 9` yielded.
INFO: after 0:00:04.500547, 0 error and 10 `integers from 0 to 9` yielded.
```

The amount of logs will never be overwhelming because they are produced logarithmically e.g. the 11th log will be produced when the iteration reaches the 1024th element.

## `.limit`
Defines a limitation on the number of parent elements yielded.

```python
ten_first_integers: Stream[int] = integers.limit(count=10)
```


---

# 📦 ***Notes Box***

## typing
This module is **typed**, you can [`mypy`](https://github.com/python/mypy) it !

## supported Python versions
This module is **compatible with Python `3.7` or newer**.

It is unittested for: `3.7.17`, `3.8.18`, `3.9.18`, `3.10.13`, `3.11.7`, `3.12.1`

## multi lines
You may find it convenient to enclose your operations in parentheses instead of using trailing backslashes `\`.

```python
(
    Stream(lambda: range(10))
    .map(lambda x: 1 / x)
    .catch(ZeroDivisionError)
    .exhaust()
)
```

## functions
`Stream`'s methods are also exposed as functions:
```python
from streamable.functions import slow
iterator: Iterator[int] = ...
slowed_iterator: Iterator[int] = slow(iterator)
```

## set logging level
```python
import logging
logging.getLogger("streamable").setLevel(logging.WARNING)
```

## visitor pattern
A `Stream` exposes an `.accept` method and you can implement your custom [***visitor***](https://en.wikipedia.org/wiki/Visitor_pattern) by extending the `streamable.visit.Visitor` class.

---

# 🔧 ***Use case in Data Engineering***

Data Engineers often need to write Python jobs to:
- **ETL**: *Extract* the data from a source API -> *Transform* it -> *Load* it into the data warehouse
- **EL**: the same but with minimal transformation
- **Reverse ETL**: *Extract* data from the data warehouse -> *Transform* it -> *Load* it into a destination API

These scripts are scheduled to run periodically (using a jobs orchestrator like *Airflow/DAGster/Prefect*) and if they only manipulate the data produced or updated during their period they should not end up manipulating huge volumes. At worst if you are an *Amazon*-sized business you may need to process 10 millions payment transactions every 10 minutes.

Some of these jobs are often very similar across companies and that is where tools like *Airbyte* shine by allowing to connect common sources and destinations to tackle most of the **EL** needs. When it's not enough Data Engineers write custom jobs.

These jobs are typically composed of:
- The definition of the data **source** using:
  - a client library: e.g. the `stripe` or `google.cloud.bigquery` modules
  - a custom `Iterator` that loops over the pages of a REST API and yields responses
  - ...

- The **transformation** functions (may involve calls to APIs).

- The function to post into a **destination** using:
  - a client library
  - the `requests` module
  - ...

- The logic to group elements into **batches** to transform or POST them at once.

- The logic to **limit the rate** of calls to APIs to avoid getting `HTTP 429 (Too Many Requests)` errors.

- The logic to perform API calls **concurrently** using threads or `asyncio`.

- The logic to **retry** calls in case of failure.

- The logic to **catch** certain exceptions.

- The logic to **log** the job's progress.

In this journey one can leverage the expressive interface provided by `streamable` to produce jobs that are easy to read and to maintain.

Here is the basic structure of a reverse ETL job leveraging `streamable`and scheduled via Airflow:

```python
from datetime import datetime
from typing import cast

from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue

@dag(schedule=None, catchup=True, start_date=datetime(2024, 2, 3))
def reverse_etl_example():

    @task.sensor(poke_interval=60)
    def users_query(
        data_interval_start = cast(datetime, ...),
        data_interval_end = cast(datetime, ...)
    ) -> PokeReturnValue:
        """
        Checks data availability for the interval and returns the corresponding query.
        """
        return PokeReturnValue(
            is_done=True,
            xcom_value=f"""
                SELECT *
                FROM users
                WHERE updated_at BETWEEN '{data_interval_start}' AND '{data_interval_end}'
            """
        )

    @task
    def post_users(users_query: str):
        """
        Iterates over users from BigQuery and POST them concurrently by batch of 100 into a third party.
        The rate limit (16 requests/s) of the third party is respected.
        If any exception happened the task will finally raise it.
        """
        from google.cloud import bigquery
        import requests
        from streamable import Stream

        (
            Stream(bigquery.Client(...).query(users_query).result)
            .map(dict)
            .observe("users")
            .batch(size=100)
            .observe("user batches")
            .slow(frequency=16)
            .map(lambda users:
                requests.post("https://third.party/users", headers=cast(dict, ...), json=users),
                concurrency=3,
            )
            .do(requests.Response.raise_for_status)
            .observe("integrated user batches")
            .catch(Exception, raise_at_exhaustion=True)
            .exhaust(explain=True)
        )
    
    post_users(users_query())

_ = reverse_etl_example()

```
