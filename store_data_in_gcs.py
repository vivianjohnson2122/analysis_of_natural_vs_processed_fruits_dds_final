import os
import json
import datetime
import requests
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import storage
from google.api_core.exceptions import Conflict


USDA_API_BASE = "https://api.nal.usda.gov/fdc/v1"

USDA_DATASETS = {
    "survey": {
        "api_path": "/foods/list",
        "params": {"dataType": ["Survey (FNDDS)"], "pageSize": 200},
        "bucket": "survey-foods-dds-final-proj",
        "blob_prefix": "survey_all",
    },
    "legacy": {
        "api_path": "/foods/list",
        "params": {"dataType": ["SR Legacy"], "pageSize": 200},
        "bucket": "legacy-foods-dds-final-proj",
        "blob_prefix": "legacy_all",
    },
    "foundation": {
        "api_path": "/foods/list",
        "params": {"dataType": ["Foundation"], "pageSize": 200},
        "bucket": "foundation-foods-dds-final-proj",
        "blob_prefix": "foundation_all",
    },
}


def store_to_gcs(service_account_key: str,
                 project_id: str,
                 bucket_name: str,
                 file_name: str,
                 data: str) -> None:
    """Upload string data to a GCS bucket, creating the bucket if needed."""
    credentials = service_account.Credentials.from_service_account_file(service_account_key)
    client = storage.Client(project=project_id, credentials=credentials)

    try:
        bucket = client.create_bucket(bucket_name)
        print(f"Bucket {bucket_name} created.")
    except Conflict:
        bucket = client.bucket(bucket_name)
        print(f"Bucket {bucket_name} already exists.")

    blob = bucket.blob(file_name)
    blob.upload_from_string(data)
    print(f"Uploaded {file_name} to {bucket_name}")


def fetch_usda_data(api_key: str, dataset_key: str) -> list:
    """Fetch all pages of a USDA FoodData Central dataset."""
    cfg = USDA_DATASETS[dataset_key]
    all_foods = []
    page = 1

    while True:
        params = {**cfg["params"], "pageNumber": page, "api_key": api_key}
        resp = requests.post(f"{USDA_API_BASE}{cfg['api_path']}", json=params)
        resp.raise_for_status()
        foods = resp.json()
        if not foods:
            break
        all_foods.extend(foods)
        print(f"  {dataset_key} page {page}: {len(foods)} items (total: {len(all_foods)})")
        page += 1

    return all_foods


def ingest_dataset_to_gcs(dataset_key: str):
    """Fetch a USDA dataset and upload the JSON to GCS."""
    load_dotenv()
    sa_key = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
    proj_id = os.getenv("GCP_PROJECT_ID")
    api_key = os.getenv("USDA_API_KEY", "DEMO_KEY")

    cfg = USDA_DATASETS[dataset_key]
    data = fetch_usda_data(api_key, dataset_key)
    file_name = f"{cfg['blob_prefix']}/{datetime.date.today()}.json"

    store_to_gcs(
        service_account_key=sa_key,
        project_id=proj_id,
        bucket_name=cfg["bucket"],
        file_name=file_name,
        data=json.dumps(data),
    )
    return len(data)


if __name__ == "__main__":
    load_dotenv()
    for key in USDA_DATASETS:
        print(f"\n--- Ingesting {key} ---")
        count = ingest_dataset_to_gcs(key)
        print(f"  -> {count} documents uploaded")
