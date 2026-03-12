import os
import json
import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
from google.oauth2 import service_account
from google.cloud import storage


USDA_COLLECTIONS = {
    "survey": {
        "bucket": "survey-foods-dds-final-proj",
        "blob_prefix": "survey_all",
        "collection": "survey-food-data-collection",
    },
    "legacy": {
        "bucket": "legacy-foods-dds-final-proj",
        "blob_prefix": "legacy_all",
        "collection": "legacy-food-data-collection",
    },
    "foundation": {
        "bucket": "foundation-foods-dds-final-proj",
        "blob_prefix": "foundation_all",
        "collection": "foundation-food-data-collection",
    },
}


def retrieve_data_from_gcs(service_account_key: str,
                           project_id: str,
                           bucket_name: str,
                           file_name: str) -> list:
    """Download JSON data from a GCS bucket."""
    credentials = service_account.Credentials.from_service_account_file(service_account_key)
    client = storage.Client(project=project_id, credentials=credentials)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = json.loads(blob.download_as_string())
    return content


def load_data_into_mongo(mongo_uri: str,
                         db_name: str,
                         collection_name: str,
                         data: list,
                         replace: bool = True):
    """Insert documents into a MongoDB collection.
    If replace=True, drops the existing collection first to avoid duplicates."""
    client = MongoClient(mongo_uri)
    db = client[db_name]

    if replace and collection_name in db.list_collection_names():
        db[collection_name].drop()
        print(f"  Dropped existing collection: {collection_name}")

    collection = db[collection_name]
    result = collection.insert_many(data)
    print(f"  Inserted {len(result.inserted_ids)} documents into {collection_name}")
    client.close()


def get_mongo_uri() -> str:
    username = os.getenv("MONGODB_ATLAS_USERNAME")
    password = os.getenv("MONGODB_ATLAS_PASSWORD")
    host = os.getenv("MONGODB_CLUSTER_HOST")
    return f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority"


def load_dataset_gcs_to_mongo(dataset_key: str, date_str: str = None):
    """Load a single USDA dataset from GCS into MongoDB."""
    load_dotenv()
    sa_key = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
    proj_id = os.getenv("GCP_PROJECT_ID")
    db_name = os.getenv("MONGODB_DB_NAME", "msds697")

    if date_str is None:
        date_str = str(datetime.date.today())

    cfg = USDA_COLLECTIONS[dataset_key]
    file_name = f"{cfg['blob_prefix']}/{date_str}.json"

    print(f"  Retrieving from GCS: {cfg['bucket']}/{file_name}")
    data = retrieve_data_from_gcs(sa_key, proj_id, cfg["bucket"], file_name)

    mongo_uri = get_mongo_uri()
    print(f"  Loading {len(data)} docs into MongoDB: {cfg['collection']}")
    load_data_into_mongo(mongo_uri, db_name, cfg["collection"], data, replace=True)

    return len(data)


if __name__ == "__main__":
    load_dotenv()
    for key in USDA_COLLECTIONS:
        print(f"\n--- Loading {key} from GCS to MongoDB ---")
        count = load_dataset_gcs_to_mongo(key, date_str="2026-03-05")
        print(f"  -> {count} documents loaded")
