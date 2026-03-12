import os
import time
from typing import List, Dict, Any, Optional
from pymongo import MongoClient
from dotenv import load_dotenv


NUTRIENT_IDS = {
    "calories": 1008,
    "protein": 1003,
    "total_fat": 1004,
    "total_sugars": 2000,
    "sodium": 1093,
    "fiber": 1079,
    "saturated_fat": 1258,
    "cholesterol": 1253,
}

HEALTH_THRESHOLDS = {
    "calories_max": 300,
    "sugars_max": 10,
    "sodium_max": 400,
    "saturated_fat_max": 5,
    "fiber_min": 3,
    "protein_min": 5,
}

HEALTHY_SCORE_MIN = 4

RAW_COLLECTIONS = [
    "survey-food-data-collection",
    "legacy-food-data-collection",
    "foundation-food-data-collection",
]


def get_mongo_client() -> MongoClient:
    load_dotenv()
    username = os.getenv("MONGODB_ATLAS_USERNAME")
    password = os.getenv("MONGODB_ATLAS_PASSWORD")
    host = os.getenv("MONGODB_CLUSTER_HOST")
    uri = f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority"
    return MongoClient(uri, serverSelectionTimeoutMS=15000)


def get_db():
    client = get_mongo_client()
    db_name = os.getenv("MONGODB_DB_NAME", "msds697")
    return client, client[db_name]


# ---------------------------------------------------------------------------
# FLATTEN + LABEL PIPELINE (core aggregation that creates training data)
# ---------------------------------------------------------------------------

def _build_flatten_and_label_pipeline() -> list:
    """Builds the MongoDB aggregation pipeline that flattens foodNutrients,
    pivots key nutrients into top-level fields, and labels healthy/unhealthy."""
    nutrient_ids = list(NUTRIENT_IDS.values())

    pivot_accumulators = {}
    for name, nid in NUTRIENT_IDS.items():
        pivot_accumulators[name] = {
            "$max": {
                "$cond": [
                    {"$eq": ["$foodNutrients.nutrientId", nid]},
                    "$foodNutrients.value",
                    None,
                ]
            }
        }

    health_score_conditions = [
        {"$cond": [{"$lt": ["$calories", HEALTH_THRESHOLDS["calories_max"]]}, 1, 0]},
        {"$cond": [{"$lt": ["$total_sugars", HEALTH_THRESHOLDS["sugars_max"]]}, 1, 0]},
        {"$cond": [{"$lt": [{"$ifNull": ["$sodium", 0]}, HEALTH_THRESHOLDS["sodium_max"]]}, 1, 0]},
        {"$cond": [{"$lt": [{"$ifNull": ["$saturated_fat", 0]}, HEALTH_THRESHOLDS["saturated_fat_max"]]}, 1, 0]},
        {"$cond": [{"$gt": [{"$ifNull": ["$fiber", 0]}, HEALTH_THRESHOLDS["fiber_min"]]}, 1, 0]},
        {"$cond": [{"$gt": [{"$ifNull": ["$protein", 0]}, HEALTH_THRESHOLDS["protein_min"]]}, 1, 0]},
    ]

    pipeline = [
        {"$unwind": "$foodNutrients"},
        {"$match": {"foodNutrients.nutrientId": {"$in": nutrient_ids}}},
        {
            "$group": {
                "_id": "$fdcId",
                "description": {"$first": "$description"},
                "dataType": {"$first": "$dataType"},
                "foodCategory": {"$first": "$foodCategory"},
                "sourceCollection": {"$first": "$_source"},
                **pivot_accumulators,
            }
        },
        {
            "$match": {
                "calories": {"$ne": None},
                "protein": {"$ne": None},
                "total_fat": {"$ne": None},
            }
        },
        {
            "$addFields": {
                "healthScore": {"$sum": health_score_conditions},
                "isHealthy": {"$gte": [{"$sum": health_score_conditions}, HEALTHY_SCORE_MIN]},
            }
        },
        {"$sort": {"healthScore": -1}},
    ]
    return pipeline


def flatten_and_label_all_foods(db, dry_run: bool = True) -> List[Dict[str, Any]]:
    """Runs the flatten+label pipeline across all 3 raw collections and
    returns combined results. When dry_run=False, writes to Atlas collections."""
    all_results = []

    for coll_name in RAW_COLLECTIONS:
        pipeline = _build_flatten_and_label_pipeline()
        # tag each document with its source collection before grouping
        source_tag = [{"$addFields": {"_source": coll_name}}]
        full_pipeline = source_tag + pipeline

        print(f"\n  Running flatten+label on {coll_name}...")
        start = time.time()
        results = list(db[coll_name].aggregate(full_pipeline, allowDiskUse=True))
        elapsed = time.time() - start
        print(f"  -> {len(results)} labeled foods ({elapsed:.2f}s)")
        all_results.extend(results)

    healthy = [r for r in all_results if r.get("isHealthy")]
    unhealthy = [r for r in all_results if not r.get("isHealthy")]
    print(f"\n  Total: {len(all_results)} foods | {len(healthy)} healthy | {len(unhealthy)} unhealthy")

    if not dry_run:
        print("\n  Writing to MongoDB Atlas...")
        for coll_target, docs in [("healthy-foods-collection", healthy),
                                   ("unhealthy-foods-collection", unhealthy)]:
            if coll_target in db.list_collection_names():
                db[coll_target].drop()
            if docs:
                for d in docs:
                    d.pop("_id", None)
                db[coll_target].insert_many(docs)
                print(f"  -> Wrote {len(docs)} docs to {coll_target}")
    else:
        print("  (DRY RUN — no writes to Atlas)")

    return all_results


# ---------------------------------------------------------------------------
# AGGREGATION: Nutrition summary by food category
# ---------------------------------------------------------------------------

def nutrition_summary_by_category(db, collection_name: str, out_collection: Optional[str] = None) -> List[Dict]:
    """Average key nutrients grouped by foodCategory."""
    pipeline = [
        {"$unwind": "$foodNutrients"},
        {"$match": {"foodNutrients.nutrientId": {"$in": list(NUTRIENT_IDS.values())}}},
        {
            "$group": {
                "_id": {
                    "foodCategory": "$foodCategory",
                    "nutrientName": "$foodNutrients.nutrientName",
                },
                "avgValue": {"$avg": "$foodNutrients.value"},
                "count": {"$sum": 1},
            }
        },
        {
            "$group": {
                "_id": "$_id.foodCategory",
                "nutrients": {
                    "$push": {
                        "nutrient": "$_id.nutrientName",
                        "avgValue": {"$round": ["$avgValue", 2]},
                        "count": "$count",
                    }
                },
                "totalFoods": {"$sum": 1},
            }
        },
        {"$sort": {"totalFoods": -1}},
    ]
    if out_collection:
        pipeline.append({"$out": out_collection})

    start = time.time()
    results = list(db[collection_name].aggregate(pipeline, allowDiskUse=True))
    elapsed = time.time() - start
    print(f"  nutrition_summary_by_category({collection_name}): {len(results)} categories ({elapsed:.2f}s)")
    return results


def cross_collection_comparison(db) -> List[Dict]:
    """Compare average calories, protein, fat, sugars across data sources."""
    all_results = []
    for coll_name in RAW_COLLECTIONS:
        pipeline = [
            {"$unwind": "$foodNutrients"},
            {"$match": {"foodNutrients.nutrientId": {"$in": [1008, 1003, 1004, 2000]}}},
            {
                "$group": {
                    "_id": "$foodNutrients.nutrientName",
                    "avgValue": {"$avg": "$foodNutrients.value"},
                    "minValue": {"$min": "$foodNutrients.value"},
                    "maxValue": {"$max": "$foodNutrients.value"},
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"_id": 1}},
        ]
        start = time.time()
        results = list(db[coll_name].aggregate(pipeline, allowDiskUse=True))
        elapsed = time.time() - start
        for r in results:
            r["collection"] = coll_name
        all_results.extend(results)
        print(f"  cross_collection_comparison({coll_name}): {len(results)} nutrient groups ({elapsed:.2f}s)")
    return all_results


# ---------------------------------------------------------------------------
# QUERIES — Raw collections
# ---------------------------------------------------------------------------

def get_all_foods(db, collection_name: str, limit: int = 0) -> List[Dict]:
    """Return all foods with description, category, and data type."""
    start = time.time()
    cursor = db[collection_name].find(
        {},
        {"_id": 0, "fdcId": 1, "description": 1, "foodCategory": 1, "dataType": 1}
    )
    if limit:
        cursor = cursor.limit(limit)
    results = list(cursor)
    elapsed = time.time() - start
    print(f"  get_all_foods({collection_name}): {len(results)} docs ({elapsed:.2f}s)")
    return results


def get_foods_by_category(db, collection_name: str, category: str) -> List[Dict]:
    """Find foods in a specific category (e.g. 'Fruits and Fruit Juices')."""
    start = time.time()
    results = list(db[collection_name].find(
        {"foodCategory": category},
        {"_id": 0, "fdcId": 1, "description": 1, "foodCategory": 1}
    ))
    elapsed = time.time() - start
    print(f"  get_foods_by_category({collection_name}, '{category}'): {len(results)} docs ({elapsed:.2f}s)")
    return results


def top_foods_by_nutrient(db, collection_name: str, nutrient_id: int, top_n: int = 10) -> List[Dict]:
    """Top N foods ranked by a specific nutrient value."""
    pipeline = [
        {"$unwind": "$foodNutrients"},
        {"$match": {"foodNutrients.nutrientId": nutrient_id}},
        {
            "$project": {
                "_id": 0,
                "fdcId": 1,
                "description": 1,
                "foodCategory": 1,
                "nutrientName": "$foodNutrients.nutrientName",
                "value": "$foodNutrients.value",
                "unit": "$foodNutrients.unitName",
            }
        },
        {"$sort": {"value": -1}},
        {"$limit": top_n},
    ]
    start = time.time()
    results = list(db[collection_name].aggregate(pipeline))
    elapsed = time.time() - start
    nutrient_name = results[0]["nutrientName"] if results else f"id={nutrient_id}"
    print(f"  top_foods_by_nutrient({collection_name}, {nutrient_name}): top {top_n} ({elapsed:.2f}s)")
    return results


def avg_nutrients_by_category(db, collection_name: str) -> List[Dict]:
    """Average calories, protein, fat, sugars per food category."""
    pipeline = [
        {"$unwind": "$foodNutrients"},
        {"$match": {"foodNutrients.nutrientId": {"$in": [1008, 1003, 1004, 2000]}}},
        {
            "$group": {
                "_id": {
                    "category": "$foodCategory",
                    "nutrient": "$foodNutrients.nutrientName",
                },
                "avgValue": {"$avg": "$foodNutrients.value"},
            }
        },
        {
            "$group": {
                "_id": "$_id.category",
                "nutrients": {
                    "$push": {
                        "name": "$_id.nutrient",
                        "avg": {"$round": ["$avgValue", 2]},
                    }
                },
            }
        },
        {"$sort": {"_id": 1}},
    ]
    start = time.time()
    results = list(db[collection_name].aggregate(pipeline, allowDiskUse=True))
    elapsed = time.time() - start
    print(f"  avg_nutrients_by_category({collection_name}): {len(results)} categories ({elapsed:.2f}s)")
    return results


def count_foods_by_category(db, collection_name: str) -> List[Dict]:
    """Count of foods per category."""
    pipeline = [
        {"$group": {"_id": "$foodCategory", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    start = time.time()
    results = list(db[collection_name].aggregate(pipeline))
    elapsed = time.time() - start
    print(f"  count_foods_by_category({collection_name}): {len(results)} categories ({elapsed:.2f}s)")
    return results


# ---------------------------------------------------------------------------
# QUERIES — Labeled (aggregated) collections
# ---------------------------------------------------------------------------

def get_healthy_foods(db, limit: int = 0) -> List[Dict]:
    """Return foods from the healthy-foods-collection."""
    cursor = db["healthy-foods-collection"].find(
        {}, {"_id": 0, "description": 1, "foodCategory": 1, "healthScore": 1, "calories": 1}
    ).sort("healthScore", -1)
    if limit:
        cursor = cursor.limit(limit)
    results = list(cursor)
    print(f"  get_healthy_foods: {len(results)} docs")
    return results


def get_unhealthy_foods(db, limit: int = 0) -> List[Dict]:
    """Return foods from the unhealthy-foods-collection."""
    cursor = db["unhealthy-foods-collection"].find(
        {}, {"_id": 0, "description": 1, "foodCategory": 1, "healthScore": 1, "calories": 1}
    ).sort("healthScore", 1)
    if limit:
        cursor = cursor.limit(limit)
    results = list(cursor)
    print(f"  get_unhealthy_foods: {len(results)} docs")
    return results


def healthy_vs_unhealthy_by_category(db) -> List[Dict]:
    """Count of healthy vs unhealthy foods per food category."""
    results = []
    for label, coll in [("healthy", "healthy-foods-collection"),
                         ("unhealthy", "unhealthy-foods-collection")]:
        pipeline = [
            {"$group": {"_id": "$foodCategory", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]
        for r in db[coll].aggregate(pipeline):
            results.append({"category": r["_id"], "label": label, "count": r["count"]})
    print(f"  healthy_vs_unhealthy_by_category: {len(results)} rows")
    return results


def avg_health_score_by_source(db) -> List[Dict]:
    """Average health score grouped by source collection (dataType)."""
    results = []
    for coll in ["healthy-foods-collection", "unhealthy-foods-collection"]:
        pipeline = [
            {
                "$group": {
                    "_id": "$sourceCollection",
                    "avgHealthScore": {"$avg": "$healthScore"},
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"avgHealthScore": -1}},
        ]
        for r in db[coll].aggregate(pipeline):
            results.append(r)
    print(f"  avg_health_score_by_source: {len(results)} rows")
    return results


# ---------------------------------------------------------------------------
# MAIN — run everything in dry-run mode (read-only)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    load_dotenv()
    client, db = get_db()

    print("=" * 60)
    print("PHASE 1: Flatten + Label Foods (DRY RUN)")
    print("=" * 60)
    labeled_foods = flatten_and_label_all_foods(db, dry_run=True)

    print("\n" + "=" * 60)
    print("PHASE 2: Aggregations")
    print("=" * 60)
    for coll in RAW_COLLECTIONS:
        nutrition_summary_by_category(db, coll)

    print("\n  --- Cross-collection comparison ---")
    cross_collection_comparison(db)

    print("\n" + "=" * 60)
    print("PHASE 3: Queries on Raw Collections")
    print("=" * 60)
    for coll in RAW_COLLECTIONS:
        print(f"\n  --- {coll} ---")
        get_all_foods(db, coll, limit=5)
        count_foods_by_category(db, coll)
        avg_nutrients_by_category(db, coll)
        top_foods_by_nutrient(db, coll, NUTRIENT_IDS["calories"], top_n=5)

    print("\n  --- Category-specific query ---")
    get_foods_by_category(db, "legacy-food-data-collection", "Fruits and Fruit Juices")

    print("\n" + "=" * 60)
    print("SAMPLE LABELED DATA")
    print("=" * 60)
    if labeled_foods:
        print("\n  Top 5 healthiest:")
        for f in labeled_foods[:5]:
            print(f"    {f['description'][:50]:50s} | score={f['healthScore']} | cal={f.get('calories', 'N/A')}")
        print("\n  Top 5 least healthy:")
        for f in labeled_foods[-5:]:
            print(f"    {f['description'][:50]:50s} | score={f['healthScore']} | cal={f.get('calories', 'N/A')}")

    client.close()
    print("\nDone. To write labeled collections to Atlas, run with dry_run=False.")
