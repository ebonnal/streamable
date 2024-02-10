
# 🔧 ***Use case in Data Engineering***

Data Engineers often need to write Python jobs to:
- **ETL**: *Extract* the data from a source -> *Transform* it -> *Load* it into the data warehouse
- **EL**: the same but with minimal transformation
- **Reverse ETL**: *Extract* data from the data warehouse -> *Transform* it -> *Load* it into a destination

These scripts are scheduled to **run periodically** using a job orchestrator like *Airflow/DAGster/Prefect* and they typically process a relatively small amount of data updated within specific time intervals.

Some of these jobs are often very similar across companies and that is where tools like *Airbyte* shine by allowing to connect common sources and destinations to tackle most of the **EL** needs. When it's not enough Data Engineers write custom jobs.

These jobs typically:
- define a data **source** using:
  - a client library: e.g. the `stripe` or `google.cloud.bigquery` modules
  - a custom `Iterator` that loops over the pages of a REST API and yields responses
  - ...

- **transform** elements (may involve calls to APIs)

- post into a **destination** using:
  - a client library
  - the `requests` module
  - ...

- group elements to transform or POST them by **batch**.

- **rate limit** the calls made to APIs to respect the request quotas and avoid `HTTP 429 (Too Many Requests)` errors.

- Make API calls **concurrently** using threads or `asyncio`.

- **retry** calls in case of failure.

- **catch** certain exceptions.

- **log** the job's progress.

The memory footprint of these jobs can be limited by an `Iterator`-based implementation: the source yields elements in small sets that are then processed on-the-fly and never collected.

In this journey one can leverage the expressive interface provided by `streamable` to produce jobs that are easy to read and to maintain.

Here is the basic structure of a reverse ETL job that uses `streamable` and is scheduled via Airflow:

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
        data_interval_end = cast(datetime, ...),
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
                requests.post("https://third.party/users", json=users, headers=cast(dict, ...)),
                concurrency=3,
            )
            .foreach(requests.Response.raise_for_status)
            .observe("integrated user batches")
            .catch(raise_at_exhaustion=True)
            .explain()
            .exhaust()
        )
    
    post_users(users_query())

_ = reverse_etl_example()

```
