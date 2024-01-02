# `streamable`

[![Actions Status](https://github.com/bonnal-enzo/streamable/workflows/unittest/badge.svg)](https://github.com/bonnal-enzo/streamable/actions) [![Actions Status](https://github.com/bonnal-enzo/streamable/workflows/typing/badge.svg)](https://github.com/bonnal-enzo/streamable/actions)
[![Actions Status](https://github.com/bonnal-enzo/streamable/workflows/lint/badge.svg)](https://github.com/bonnal-enzo/streamable/actions) [![Actions Status](https://github.com/bonnal-enzo/streamable/workflows/PyPI/badge.svg)](https://github.com/bonnal-enzo/streamable/actions)

Ease the manipulation of `Iterable`s.

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

Instantiate a `Stream` by providing a function that returns an `Iterable` (the data source).

## 4. declare operations

A `Stream` is ***immutable***, meaning that applying an operation returns a new child stream while the parent stream remains unchanged.

There are 2 kinds of operations:
- **transformations**: to act on the stream's elements
- **controls**: to configure the behaviors of the iteration over the stream


```python
odd_squares: Stream[int] = (
    integers
    .map(lambda x: x ** 2, concurrency=2) # transformation
    .filter(lambda x: x % 2 == 1) # transformation
    .slow(frequency=10) # control
)
```
All operations are described in the ***Operations guide*** section.

## 5. iterate

Once your stream's declaration is done you can iterate over it. Our `Stream[int]` being an `Iterable[int]`, you are free to iterate over it the way you want, e.g.:
```python
set(rate_limited_odd_squares)
```
```python
sum(rate_limited_odd_squares)
```
```python
for i in rate_limited_odd_squares:
    ...
```

Alternatively, a stream also exposes an `.exhaust` method to launch an iteration over itself until exhaustion, with doing anything with output elements.

```python
rate_limited_odd_squares.exhaust()
```



---

# 📒 ***operations guide***

Let's keep the same example:
```python
integers = Stream(lambda: range(10))
```

# transformations
![](./img/transform.gif)

## `.map`
Defines the application of a function on parent elements.
```python
integer_strings: Stream[str] = integers.map(str)
```

It has an optional `concurrency` parameter if you need to apply the function concurrently using multiple threads.

## `.do`
Defines the application of a function on parent elements like `.map`, but the parent elements will be forwarded instead of the result of the function.

```python
printed_integers: Stream[int] = integers.do(print)
```

It also has an optional `concurrency` parameter.

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

In this example a batch will be a list of 100 elements.

It may contain less elements in the following cases:
- the stream is exhausted
- an exception occurred
- more than 60 `seconds` have elapsed since the last batch has been yielded.

## `.flatten`

Defines the ungrouping of parent elements assuming that the parent elements are `Iterable`s.

```python
integers: Stream[int] = integer_batches.flatten()
```

It also has an optional `concurrency` parameter to flatten concurrently several parent iterables.

## `.chain`

Defines the concatenation of the parent stream with other streams. The resulting stream yields the elements of one stream until it is exhausted and then moves to the next one. It starts with the stream on which `.chain` is called.

```python
one_to_ten_integers: Stream[int] = Stream(lambda: range(1, 11))
eleven_to_twenty_integers: Stream[int] = Stream(lambda: range(11, 21))
twenty_one_to_thirty_integers: Stream[int] = Stream(lambda: range(21, 31))

one_to_thirty_integers: Stream[int] = one_to_ten_integers.chain(
    eleven_to_twenty_integers,
    twenty_one_to_thirty_integers,
)
```

# controls
![](./img/control.gif)

## `.slow`

Defines a maximum rate at which parent elements will be yielded.

```python
slowed_integers: Stream[int] = integers.slow(frequency=2)
```

The rate is expressed in elements per second, here a maximum of 2 elements per second will be yielded when iterating on the stream.

## `.observe`

Defines that the iteration process will be logged.

```python
observed_slowed_integers: Stream[int] = slowed_integers.observe(what="integers from 0 to 9")
```

When iterating over the stream, you should get an output like:

```
INFO - iteration over 'integers from 0 to 9' will be logged.
INFO - 1 integers from 0 to 9 have been yielded, in elapsed time 0:00:00.000283, with 0 error produced
INFO - 2 integers from 0 to 9 have been yielded, in elapsed time 0:00:00.501373, with 0 error produced
INFO - 4 integers from 0 to 9 have been yielded, in elapsed time 0:00:01.501346, with 0 error produced
INFO - 8 integers from 0 to 9 have been yielded, in elapsed time 0:00:03.500864, with 0 error produced
INFO - 10 integers from 0 to 9 have been yielded, in elapsed time 0:00:04.500547, with 0 error produced
```

As you can notice the logs can never be overwhelming because they are produced logarithmically.


## `.catch`

Defines that the provided type of exception will be catched.

```python
inverse_floats: Stream[float] = integers.map(lambda x: 1/x)
safe_inverse_floats: Stream[float] = inverse_floats.catch(ZeroDivisionError)
```

It has optional parameters:
- `when`: a function that takes the parent element as input and decides whether or not to catch the exception.
- `raise_at_exhaustion`: a boolean that you can set to `True` if you want the first catched exception to be raised when the stream is exhausted (i.e. at the end of the iteration).

---

# 🔧 ***typical use case in Data Engineering***
![](./img/dataeng.gif)

As a data engineer, you often need to write python scripts to do **ETL** (*Extract* the data from a source API, *Transform* and *Load* it into the data warehouse) or **EL** (same but with minimal transformation) or **Reverse ETL** (read data from the data warehouse and post it into a destination API).

These scripts **do not manipulate huge volumes** of data because they are scheduled to iterate periodically (using orchestrators like *Airflow/DAGster/Prefect*) and only manipulates the data produced or updated during that period. At worst if you are *Amazon*-sized business you may need to process 10 millions payment transactions every 10 minutes.

These scripts tend to be replaced in part by EL tools like *Airbyte*, but sometimes you still need **custom integration logic**.

These scripts are typically composed of:
- The definition of a data **source** that may use:
  - A client library: e.g. the `stripe` or `google.cloud.bigquery` modules.
  - A custom `Iterator` that loops over the pages of a REST API and yields `Dict[str, Any]` json responses.
  - ...

- The **transformation** functions, that again may involve to call APIs.

- The function to post into a **destination** that may use:
  - A client library.
  - The `requests` module.

- The logic to **batch** some records together: it will often costs less to POST several records at once to an API.

- The logic to **limit the rate** of the calls to APIs to avoid breaking the API quotas (leading to the infamous `HTTP 429 (Too Many Requests)` status codes).

- The logic to make concurrent calls to APIs: `asyncio` can be very performant, but it often turns out that spawning a few **threads** using a `ThreadPoolExecutor` is enough and more flexible.

- The **retry** logic: be gentle with APIs, 2 retries each waiting 5 seconds can definitely help. For this the [`retrying` module](https://github.com/rholder/retrying) is great and let you decorate your transformation and destination functions with a retrying logic.

- The logic to **catch** exceptions of a given type. Also, we typically want to catch errors and seamlessly proceed with the integration until completion. For instance, if you have 1000 records to integrate and encounter an exception at the 236th record due to a malformed record, it is often more favorable to successfully integrate 999 records and raise after the interation has completed compared to skipping 763 valid records prematurely.

The ambition of `streamable` is to help us write these type of scripts in a **DRY** (Don't Repeat Yourself), **flexible**, **robust** and **readable** way.

Let's delve into an example to gain a better understanding of what a job using `streamable` entails!

## 1. imports
```python
import datetime
import requests
from streamable import Stream
from google.cloud import bigquery
from typing import Iterable, Iterator, Dict, Any
```

## 2. source
Define your source `Iterable`:

```python
class PokemonCardPageSource(Iterable[List[Dict[str, Any]]]):
    def __init__(
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        page_size: int = 100,
    ):
        ...
    def __iter__() -> Iterator[List[Dict[str, Any]]]:
        # yield the pokemon cards from pokemontcg.io that
        # have been created between start_time and end_time
        page = 1
        while True:
            response: requests.Response = requests.get(...)
            response.raise_for_status()
            cards_page: List[Dict[str, Any]] = ...
            yield cards_page
            if no_more_pages:
                break
            page += 1
```

## 3. utilities

We will further need a function that raises in case there is errors in the `Dict`s we pass to it:

```python
def raise_for_errors(dct: Dict[str, Any]) -> None:
    if errors := dct["errors"]:
        raise RuntimeError(f"Errors occurred: {errors}")
```

Also let's init a BQ client:
```python
bq_client = bigquery.Client(project)
```

## 4. stream

Write your integration function.

Tip: Define your stream between parentheses to be allowed to go to line between each operation.

```python
def integrate_pokemon_cards_into_bigquery(
    start_time: datetime.datetime,
    end_time: datetime.datetime,
) -> None:
    (
        Stream(lambda: PokemonCardSource(start_time, end_time))
        # at this point we have a Stream[List[Dict[str, Any]]]

        # Let's say pokemontcg.io rate limits us to 10 calls per second,
        # let's keep a margin and slow our stream down to 9.
        .slow(frequency=9)
        .observe(what="pokemon cards page")

        # let's flatten the card page into individual cards
        .flatten()
        # at this point we have a Stream[Dict[str, Any]]

        # let's structure our row
        .map(lambda card:
            {
                "name": card["name"],
                "set": card["set"]["id"],
                "fetched_at": datetime.utcnow().isoformat(),
            }
        )
        .observe(what="transformed pokemon card")

        # Let's batch cards by 1000 for performant multi-rows insert.
        .batch(size=1000)
        # at this point we have a Stream[List[Dict[str, Any]]]
        .observe(what="pokemon card batches")

        # Let's post the batches into BQ concurrently using 2 threads.
        .map(lambda cards_batch:
            bq_client.insert_rows_json(
                    table="ingestion.pokemon_card",
                    json_rows=cards_batch,
            ),
            concurrency=2,
        )
        # at this point we have a Stream[Sequence[Dict[str, Any]]]

        # The insertion in bigquery returns a list of inserts results.
        # Let's raise if the insertion got errors.
        .flatten()
        .observe(what="bigquery insert results")
        # at this point we have a Stream[Dict[str, Any]]
        .do(raise_for_errors)

        # iterate until no more card in the stream and finally raises if errors occurred.
        .catch(Exception, raise_at_exhaustion=True)
        .exhaust()
    )
```

## 5. orchestrate
You can now wrap this script as a task within your chosen job orchestrator.

Example using **Airflow**:

```python
... # imports from 1.
from typing import Optional
from airflow.decorators import dag, task

@dag(
    default_args={
        "retries": 1,
        "execution_timeout": datetime.timedelta(minutes=5),
    },
    schedule="@weekly",
    start_date=pendulum.datetime(...),
    catchup=True,
    max_active_runs=1,
)
def weekly_integrate_pokemon_cards_in_bigquery():
    @task
    def integrate(
        data_interval_start: Optional[datetime.datetime] = None,
        data_interval_end: Optional[datetime.datetime] = None
    ):
        integrate_pokemon_cards_into_bigquery(
            start_time=data_interval_start,
            end_time=data_interval_end,
        )

    integrate()

_ = weekly_integrate_pokemon_cards_in_bigquery()
```

And we are done !

---


# 📦 ***tips Box***

## typing
`streamable` is a fully typed module, you can `mypy` it !

## multi lines declaration
You may use parenthesis instead of trailing backslash `\` to go to line between operation declarations.  
```python
(
    Stream(lambda: range(10))
    .map(lambda x: 1/x)
    .catch(ZeroDivisionError)
    .exhaust()
)
```

## `streamable`'s functions
The functionalities exposed by the `Stream` class are also available as functions:
```python
from streamable.functions import slow
iterator: Iterator[int] = ...
slowed_iterator: Iterator[int] = slow(iterator)
```

## mute logs
`.observe` produces `INFO` level logs that you can mute as follows:
```python
import logging
logging.getLogger("streamable").setLevel(logging.WARNING)
```

## dev commands cheat sheet

###  setup
after having forked `streamable` and `git clone`d it locally:
```bash
cd streamable
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

### unittest
```bash
python -m unittest
```

### type checking
```bash
python -m mypy streamable tests
```

### lint
```bash
python -m autoflake --in-place --remove-all-unused-imports --remove-unused-variables --ignore-init-module -r streamable tests \
&& python -m isort streamable tests \
&& python -m black streamable tests
```
