# necessary imports 
import requests 
import numpy as np 
import pandas as pd
import os 
import datetime
import json 
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import storage
from google.api_core.exceptions import Conflict

"""
    Uploads data to Google Cloud Storage, creating the bucket if it does not exist.

    Args:
        service_account_key (str): Path to the service account JSON key file
            used for authentication.
        project_id (str): Google Cloud project ID where the bucket resides
            or will be created.
        bucket_name (str): Name of the Google Cloud Storage bucket. If the
            bucket does not exist, it will be created.
        file_name (str): Name of the object (file) to create in the bucket.
        data (str): String data to upload to the bucket object.

"""
def store_to_gcs(service_account_key: str,
                 project_id: str,
                 bucket_name: str,
                 file_name: str,
                 data: str) -> None: 
    # get credentials 
    credentials = service_account.Credentials.from_service_account_file(service_account_key)
    client = storage.Client(project=project_id,
                            credentials=credentials)
    
    # create bucket if it doesn't exist
    try:
        bucket = client.create_bucket(bucket_name)
        print(f"Bucket {bucket_name} created.")
    except Conflict:
        bucket = client.bucket(bucket_name)
        print(f"Bucket {bucket_name} already exists.")

    blob = bucket.blob(file_name)
    blob.upload_from_string(data)

    print(f"Uploaded {file_name} to {bucket_name}")


if __name__ == "__main__":
    # fruityvice json data 
    fruit_url = 'https://www.fruityvice.com/api/fruit/all'
    fruit_response = requests.get(fruit_url)
    # store fruityvice data in json
    fruit_response_json = fruit_response.json()
    
    # open fruit facts snack data
    snack_url = "https://world.openfoodfacts.org/cgi/search.pl"
    # params to filter nutritional info from snacks 
    params = {
        "search_terms": "",
        "tagtype_0": "categories",
        "tag_contains_0": "contains",
        "tag_0": "snacks",
        "json": 1,
        "page_size": 100,
        "fields": "product_name,ingredients_text,nutriscore_grade,sugars_100g,fat_100g,additives_n"
    }
    # store in json 
    snack_response = requests.get(snack_url, params=params)
    snack_response_json = snack_response.json()

    # loading in env vars for service acc and project 
    load_dotenv()
    # keys, proj id
    sa_key = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
    proj_id = os.getenv('GCP_PROJECT_ID')
    # bucket names 
    fruit_bucket_name = 'fruit-data-dds-final-proj'
    snack_bucket_name = 'snack-data-dds-final-proj'
    # file names 
    fruit_file_name = f'fruit_all/{datetime.date.today()}.json'
    snack_file_name = f'snack_all/{datetime.date.today()}.json'
    # store fruityvice data to gcs
    store_to_gcs(service_account_key=sa_key,
                project_id=proj_id,
                bucket_name=fruit_bucket_name,
                file_name=fruit_file_name,
                data=json.dumps(fruit_response_json))
    # store snack data to gcs
    store_to_gcs(service_account_key=sa_key,
                 project_id=proj_id,
                 bucket_name=snack_bucket_name,
                 file_name=snack_file_name,
                 data=json.dumps(snack_response_json))