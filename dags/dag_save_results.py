"""
DAG 3: Save ML results to Google Cloud Storage.
    Local output/ directory -> GCS bucket

Uploads model metrics, predictions, and query timing results
from the local output directory to a GCS bucket.
"""

import os
import sys
import datetime
from pathlib import Path

from airflow.sdk import dag, task

PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

GCS_BUCKET = "survey-foods-dds-final-proj"
OUTPUT_FILES = ["model_metrics.json", "predictions.csv", "query_timing.json"]


@dag(
    dag_id="save_results_to_gcs",
    schedule=None,
    start_date=datetime.datetime(2026, 3, 1),
    catchup=False,
    tags=["usda", "gcs", "results"],
    default_args={
        "owner": "dds-group",
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=2),
    },
)
def save_results_dag():

    @task()
    def upload_file_to_gcs(filename: str) -> str:
        """Upload a single file from output/ to GCS."""
        from dotenv import load_dotenv
        from google.oauth2 import service_account
        from google.cloud import storage

        load_dotenv()
        fpath = os.path.join(PROJECT_ROOT, "output", filename)
        if not os.path.exists(fpath):
            raise FileNotFoundError(f"File not found: {fpath}")

        sa_key = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
        project_id = os.getenv("GCP_PROJECT_ID")
        credentials = service_account.Credentials.from_service_account_file(sa_key)
        client = storage.Client(project=project_id, credentials=credentials)

        blob_path = f"ml_output/{filename}"
        bucket = client.bucket(GCS_BUCKET)
        bucket.blob(blob_path).upload_from_filename(fpath)
        gcs_path = f"gs://{GCS_BUCKET}/{blob_path}"
        print(f"Uploaded {filename} -> {gcs_path}")
        return gcs_path

    for fname in OUTPUT_FILES:
        upload_file_to_gcs(fname)


save_results_dag()
