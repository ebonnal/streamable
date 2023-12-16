from operator import itemgetter

import requests
from google.cloud import bigquery, translate # type: ignore
from kioss import Pipe

(
    # Read the comments made on your platform from your BigQuery datawarehouse
    Pipe(bigquery.Client().query("SELECT text FROM fact.comment").result)
    .map(itemgetter("text"))
    .observe(what="comments")

    # translate them in english concurrently using 4 threads
    # by batch of 20 at a maximum rate of 50 batches per second
    .batch(size=20)
    .slow(freq=50)
    .map(translate.Client("en").translate, n_threads=4)
    .flatten()
    .map(itemgetter("translatedText"))
    .observe(what="comments translated in english")

    # keep only the ones containing christmas
    .filter(lambda comment: "christmas" in comment.lower())
    .observe(what="christmas comments")

    # POST these comments in some endpoint using 2 threads
    # by batch of maximum size 100 and with at least 1 batch every 10 seconds
    # and at a maximum rate of 5 batches per second.
    # Also raise if the status code is not 200.
    .batch(size=100, period=10)
    .slow(freq=5)
    .map(
        lambda comment: requests.post(
            "https://some.endpoint/comment",
            json={"text": comment},
            auth=("foo", "bar"),
        ),
        n_threads=2,
    )
    .do(requests.Response.raise_for_status)
    .map(requests.Response.text)
    .observe(what="integration response's texts")
    .run()
)
