import json 
from pymongo import MongoClient
from pymongo.errors import OperationFailure
import os 
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import storage
import requests
import datetime 

"""
    Retrieves the data in JSON format from the specified GCS bucket.

    Args:
        service_account_key (str): Path to the service account JSON key file
            used for authentication.
        project_id (str): Google Cloud project ID where the bucket resides
            or will be created.
        bucket_name (str): Name of the Google Cloud Storage bucket. If the
            bucket does not exist, it will be created.
        file_name (str): Name of the object (file) to create in the bucket.
"""
def retrieve_data_from_gcs(service_account_key: str,
                           project_id: str,
                           bucket_name: str,
                           file_name: str) -> list:
    credentials = service_account.Credentials.from_service_account_file(service_account_key)
    client = storage.Client(project=project_id,
                            credentials=credentials)
    bucket = client.bucket(bucket_name)
    file = bucket.blob(file_name)
    content = json.loads(file.download_as_string())
    return content

"""
    Load data (a list of dictionaries) into MongoDB database 
    Checks if the collection name exists, if it does - adds it, if not - creates
    and adds the data into collection

    Args:
        mongo_uri (str): The MongoDB connection URI (e.g., "mongodb://localhost:27017").
        db_name (str): Name of the database where the collection resides or will be created.
        collection_name (str): Name of the collection to insert data into.
        data (list of dict): A list of dictionaries representing documents to insert.

"""
def load_data_into_mongo(mongo_uri,
                         db_name,
                         collection_name,
                         data):
    try:
        # connect to mongodb atlast cluster 
        client = MongoClient(mongo_uri)
        db = client[db_name]
        if collection_name in db.list_collection_names():
            # collection exists already
            print(f'The collection {collection_name} exists... adding into {collection_name}')
        else: 
            # collection doesn't exist - create  
            print(f'The collection {collection_name} already exists ...creating and adding into {collection_name}')
            db.create_collection(collection_name)
        
        collection = db[collection_name]
        result = collection.insert_many(data)
        print(f"Inserted {len(result.inserted_ids)} documents")

        # close connection 
        client.close()
        print('MongoDB Connection Closed')
    
    except Exception as e:
        print(e)


if __name__ == "__main__":
    # ---------------------
    # SETTING ENVIRONMENT VARS 
    # ---------------------
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

    # ---------------------
    # GET DATA FROM GCS
    # ---------------------
    fruit_data = retrieve_data_from_gcs(service_account_key=sa_key,
                                        project_id=proj_id,
                                        bucket_name=fruit_bucket_name,
                                        file_name=fruit_file_name)
    snack_data = retrieve_data_from_gcs(service_account_key=sa_key,
                                        project_id=proj_id,
                                        bucket_name=snack_bucket_name,
                                        file_name=snack_file_name)
    
    # ---------------------
    # MONGO CONFIGURATIONS (make sure database name matches)
    # ---------------------
    MONGO_ATLAS_USERNAME = os.getenv("MONGODB_ATLAS_USERNAME")
    MONGO_ATLAS_PASSWORD = os.getenv("MONGODB_ATLAS_PASSWORD")
    MONGO_CLUSTER_URI = "test-cluster.kay1d48.mongodb.net"
    DB_NAME = "msds697"
    MONGO_URI = f"mongodb+srv://{MONGO_ATLAS_USERNAME}:{MONGO_ATLAS_PASSWORD}@{MONGO_CLUSTER_URI}/{DB_NAME}?retryWrites=true&w=majority"


    # ---------------------
    # LOAD DATA INTO MONGODB  
    # ---------------------
    load_data_into_mongo(mongo_uri=MONGO_URI,
                         db_name=DB_NAME,
                         collection_name='fruit-data-collection',
                         data=fruit_data)
    
    load_data_into_mongo(mongo_uri=MONGO_URI,
                         db_name=DB_NAME,
                         collection_name='snack-data-collection',
                         data=snack_data)
