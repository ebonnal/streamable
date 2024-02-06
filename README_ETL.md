
# 🔧 ***Use case in Data Engineering***

Data Engineers often need to write Python jobs to:
- **ETL**: *Extract* the data from a source API -> *Transform* it -> *Load* it into the data warehouse
- **EL**: the same but with minimal transformation
- **Reverse ETL**: *Extract* data from the data warehouse -> *Transform* it -> *Load* it into a destination API

These scripts are scheduled to run periodically (using a jobs orchestrator like *Airflow/DAGster/Prefect*) and if they only manipulate the data produced or updated during their period they should not end up manipulating huge volumes. At worst if you are an *Amazon*-sized business you may need to process 10 millions payment transactions every 10 minutes.

Some of these jobs are often very similar across companies and that is where tools like *Airbyte* shine by allowing to connect common sources and destinations to tackle most of the **EL** needs. When it's not enough Data Engineers write custom jobs.

These jobs typically:
- define a data **source** using:
  - a client library: e.g. the `stripe` or `google.cloud.bigquery` modules
  - a custom `Iterator` that loops over the pages of a REST API and yields responses
  - ...

- **transforms** elements (may involve calls to APIs)

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
    def users_updated_in_interval(
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
    def post_users(users_updated_in_interval: str):
        """
        Iterates over users from BigQuery and POST them concurrently by batch of 100 into a third party.
        The rate limit (16 requests/s) of the third party is respected.
        If any exception happened the task will finally raise it.
        """
        from google.cloud import bigquery
        import requests
        from streamable import Stream

        (
            Stream(bigquery.Client(...).query(users_updated_in_interval).result)
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
            .exhaust(explain=True)
        )
    
    post_users(users_updated_in_interval())

_ = reverse_etl_example()

```
