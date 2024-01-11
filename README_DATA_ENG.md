# 🔧 ***Usage in Data Engineering***
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

- The logic to **batch** some records together: it is often more efficient to POST several records at once to an API.

- The logic to **limit the rate** of the calls to APIs to avoid breaking the API quotas (leading to the infamous `HTTP 429 (Too Many Requests)` status codes).

- The logic to make concurrent calls to APIs: `asyncio` can be very performant, but it often turns out that spawning a few **threads** using a `ThreadPoolExecutor` is enough and more flexible.

- The **retry** logic: be gentle with APIs, 2 retries each waiting 5 seconds can definitely help. For this the [`retrying` module](https://github.com/rholder/retrying) is great and let you decorate your transformation and destination functions with a retrying logic.

- The logic to **catch** exceptions of a given type, and if any raise after the integration completion.

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
        .exhaust(explain=True)
    )
```

## 5. orchestrate
You can now wrap this script as a task within your chosen job orchestrator.

Example using **Airflow**:

```python
...
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
