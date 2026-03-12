"""
DAG 1: Ingest USDA food data.
    USDA FoodData Central API -> Google Cloud Storage -> MongoDB Atlas

Fetches survey, legacy, and foundation datasets from the USDA API,
stores raw JSON in GCS, then loads into MongoDB Atlas collections.
"""

import sys
import datetime
from pathlib import Path

from airflow.sdk import dag, task

PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


@dag(
    dag_id="usda_ingest_pipeline",
    schedule=None,
    start_date=datetime.datetime(2026, 3, 1),
    catchup=False,
    tags=["usda", "ingest", "gcs", "mongodb"],
    default_args={
        "owner": "dds-group",
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=2),
    },
)
def usda_ingest_pipeline():

    @task()
    def fetch_and_store_gcs(dataset_key: str) -> int:
        from store_data_in_gcs import ingest_dataset_to_gcs
        count = ingest_dataset_to_gcs(dataset_key)
        return count

    @task()
    def load_gcs_to_mongodb(dataset_key: str, doc_count: int) -> int:
        from load_into_mongodb import load_dataset_gcs_to_mongo
        loaded = load_dataset_gcs_to_mongo(dataset_key)
        return loaded

    @task()
    def verify_collections(load_results: list) -> dict:
        """Runs after all loads complete. Param creates the Airflow dependency."""
        from aggregate_and_query import get_db

        client, db = get_db()
        counts = {}
        for coll in db.list_collection_names():
            counts[coll] = db[coll].estimated_document_count()
        client.close()
        print(f"Collection counts: {counts}")
        return counts

    load_results = []
    for ds_key in ["survey", "legacy", "foundation"]:
        gcs_count = fetch_and_store_gcs(ds_key)
        loaded = load_gcs_to_mongodb(ds_key, gcs_count)
        load_results.append(loaded)

    verify_collections(load_results)


usda_ingest_pipeline()
