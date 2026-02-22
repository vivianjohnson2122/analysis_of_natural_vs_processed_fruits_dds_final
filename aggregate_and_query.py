from pymongo import MongoClient
from typing import List, Dict, Any


def get_collection(MONGO_URI: str, DB_NAME: str, COLLECTION_NAME: str):
    """
    Establishes connection to MongoDB and returns the collection.
    DO NOT MODIFY THIS FUNCTION.
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[COLLECTION_NAME]



# AGGREGATIONS - creates and stores new datasets

def summary_nutrition_total_fruit(MONGO_URI: str, DB_NAME: str, COLLECTION_NAME: str) -> List[Dict[str, Any]]:
    """
    Summary statistics for fruit dataset.
    Returns average calories, fat, sugar, carbs, and protein across all fruits.
    Stores results in fruit-nutrition-summary collection using $out.
    """
    collection = get_collection(MONGO_URI, DB_NAME, COLLECTION_NAME)

    pipeline = [
        {
            "$group": {
                "_id": None,
                "avg_calories": {"$avg": "$nutritions.calories"},
                "avg_fat": {"$avg": "$nutritions.fat"},
                "avg_sugar": {"$avg": "$nutritions.sugar"},
                "avg_carbs": {"$avg": "$nutritions.carbohydrates"},
                "avg_protein": {"$avg": "$nutritions.protein"},
                "total_fruits": {"$sum": 1}
            }
        },
        {"$out": "fruit-nutrition-summary"}
    ]

    return list(collection.aggregate(pipeline))


def aggregate_fruits_by_family(MONGO_URI: str, DB_NAME: str, COLLECTION_NAME: str) -> List[Dict[str, Any]]:
    """
    Groups fruits by botanical family.
    Returns count of fruits and list of fruit names per family, sorted by count descending.
    Stores results in fruit-family-aggregates collection using $out.
    """
    collection = get_collection(MONGO_URI, DB_NAME, COLLECTION_NAME)

    pipeline = [
        {
            "$group": {
                "_id": "$family",
                "count": {"$sum": 1},
                "fruits": {"$push": "$name"},
                "orders": {"$addToSet": "$order"}
            }
        },
        {"$sort": {"count": -1}},
        {"$out": "fruit-family-aggregates"}
    ]

    return list(collection.aggregate(pipeline))


# QUERIES - FRUIT ORIGINAL COLLECTION

def get_all_fruits(MONGO_URI: str, DB_NAME: str, COLLECTION_NAME: str) -> List[Dict[str, Any]]:
    """
    Returns all fruits with name, family, and order fields.

    Returns:
        List[Dict]: e.g. [{"name": "Banana", "family": "Musaceae", "order": "Zingiberales"}, ...]
    """
    collection = get_collection(MONGO_URI, DB_NAME, COLLECTION_NAME)
    return list(collection.find({}, {"_id": 0, "name": 1, "family": 1, "order": 1}))


def get_fruits_by_order(MONGO_URI: str, DB_NAME: str, COLLECTION_NAME: str, order: str) -> List[Dict[str, Any]]:
    """
    Finds all fruits that belong to a specific botanical order.

    Args:
        order (str): The botanical order to filter by (e.g. "Rosales")

    Returns:
        List[Dict]: List of fruits in that order
    """
    collection = get_collection(MONGO_URI, DB_NAME, COLLECTION_NAME)
    return list(collection.find({"order": order}, {"_id": 0, "name": 1, "family": 1, "order": 1}))


# QUERIES - SNACK ORIGINAL COLLECTION

def get_snacks_with_no_additives(MONGO_URI: str, DB_NAME: str, COLLECTION_NAME: str) -> List[Dict[str, Any]]:
    """
    Finds all snacks that have zero additives.

    Returns:
        List[Dict]: List of snacks with no additives
    """
    collection = get_collection(MONGO_URI, DB_NAME, COLLECTION_NAME)
    return list(collection.find({"additives_n": 0}, {"_id": 0, "product_name": 1, "nutriscore_grade": 1, "fat_100g": 1}))



# QUERIES - AGGREGATE COLLECTIONS

def get_families_with_multiple_fruits(MONGO_URI: str, DB_NAME: str, COLLECTION_NAME: str) -> List[Dict[str, Any]]:
    """
    Queries the fruit-family-aggregates collection for families with more than one fruit.

    Returns:
        List[Dict]: List of families with count > 1
    """
    collection = get_collection(MONGO_URI, DB_NAME, COLLECTION_NAME)
    return list(collection.find({"count": {"$gt": 1}}, {"_id": 1, "count": 1, "fruits": 1}))



if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()

    MONGO_ATLAS_USERNAME = os.getenv("MONGODB_ATLAS_USERNAME")
    MONGO_ATLAS_PASSWORD = os.getenv("MONGODB_ATLAS_PASSWORD")
    MONGO_CLUSTER_URI = "test-cluster.kay1d48.mongodb.net"
    DB_NAME = "msds697"
    MONGO_URI = f"mongodb+srv://{MONGO_ATLAS_USERNAME}:{MONGO_ATLAS_PASSWORD}@{MONGO_CLUSTER_URI}/{DB_NAME}?retryWrites=true&w=majority"

    # run aggregations and store new collections
    summary_nutrition_total_fruit(MONGO_URI, DB_NAME, "fruit-data-collection")
    aggregate_fruits_by_family(MONGO_URI, DB_NAME, "fruit-data-collection")
