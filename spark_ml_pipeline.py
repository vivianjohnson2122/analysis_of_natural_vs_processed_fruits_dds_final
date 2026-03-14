import os
import time
import json
from dotenv import load_dotenv
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml import Pipeline

from aggregate_and_query import (
    NUTRIENT_IDS, HEALTH_THRESHOLDS, HEALTHY_SCORE_MIN, RAW_COLLECTIONS,
)

load_dotenv()

MONGO_URI = (
    f"mongodb+srv://{os.getenv('MONGODB_ATLAS_USERNAME')}"
    f":{os.getenv('MONGODB_ATLAS_PASSWORD')}"
    f"@{os.getenv('MONGODB_CLUSTER_HOST')}"
    f"/?retryWrites=true&w=majority"
)
DB_NAME = os.getenv("MONGODB_DB_NAME", "msds697")

FEATURE_COLS = list(NUTRIENT_IDS.keys())

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")


# ── helpers ──────────────────────────────────────────────────────────────────

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("USDA_Food_Classification")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.maxResultSize", "2g")
        .getOrCreate()
    )


def load_collection_pymongo(collection_name: str) -> list:
    """Pull documents from MongoDB Atlas via pymongo."""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
    db = client[DB_NAME]
    docs = list(db[collection_name].find({}, {"_id": 0}))
    client.close()
    return docs


# ── Step 1: Load MongoDB → Spark DataFrames ─────────────────────────────────

def load_raw_dataframes(spark: SparkSession) -> dict:
    """Load all 3 raw collections into Spark DataFrames."""
    print("\n" + "=" * 60)
    print("STEP 1: Loading MongoDB collections into Spark DataFrames")
    print("=" * 60)
    dfs = {}
    for coll_name in RAW_COLLECTIONS:
        start = time.time()
        docs = load_collection_pymongo(coll_name)
        load_time = time.time() - start

        short_name = coll_name.replace("-data-collection", "").replace("-", "_")

        nid_to_name = {nid: name for name, nid in NUTRIENT_IDS.items()}
        rows = []
        for doc in docs:
            nutrients = {n: None for n in NUTRIENT_IDS}
            for fn in doc.get("foodNutrients", []):
                name = nid_to_name.get(fn.get("nutrientId"))
                if name is not None:
                    nutrients[name] = float(fn.get("value", 0))
            rows.append({
                "fdcId": doc.get("fdcId"),
                "description": doc.get("description", ""),
                "foodCategory": doc.get("foodCategory", "Unknown"),
                "dataType": doc.get("dataType", ""),
                "sourceCollection": coll_name,
                **nutrients,
            })

        schema = StructType([
            StructField("fdcId", IntegerType(), True),
            StructField("description", StringType(), True),
            StructField("foodCategory", StringType(), True),
            StructField("dataType", StringType(), True),
            StructField("sourceCollection", StringType(), True),
        ] + [StructField(n, DoubleType(), True) for n in NUTRIENT_IDS])

        df = spark.createDataFrame(rows, schema=schema)
        df.cache()
        count = df.count()
        dfs[short_name] = df
        print(f"  {coll_name}: {count} rows ({load_time:.2f}s load, "
              f"{len(rows)} docs parsed)")

    all_foods = dfs[list(dfs.keys())[0]]
    for key in list(dfs.keys())[1:]:
        all_foods = all_foods.unionByName(dfs[key])
    all_foods.cache()
    dfs["all_foods"] = all_foods
    print(f"  Combined all_foods: {all_foods.count()} rows")

    return dfs


# ── Step 2: Label healthy / unhealthy in Spark ──────────────────────────────

def label_foods(df):
    """Apply the same health-scoring logic as the MongoDB pipeline."""
    print("\n" + "=" * 60)
    print("STEP 2: Labeling foods as healthy / unhealthy")
    print("=" * 60)

    labeled = df.filter(
        F.col("calories").isNotNull() &
        F.col("protein").isNotNull() &
        F.col("total_fat").isNotNull()
    )

    labeled = labeled.withColumn("healthScore",
        (F.when(F.col("calories") < HEALTH_THRESHOLDS["calories_max"], 1).otherwise(0)) +
        (F.when(F.col("total_sugars") < HEALTH_THRESHOLDS["sugars_max"], 1).otherwise(0)) +
        (F.when(F.coalesce(F.col("sodium"), F.lit(0)) < HEALTH_THRESHOLDS["sodium_max"], 1).otherwise(0)) +
        (F.when(F.coalesce(F.col("saturated_fat"), F.lit(0)) < HEALTH_THRESHOLDS["saturated_fat_max"], 1).otherwise(0)) +
        (F.when(F.coalesce(F.col("fiber"), F.lit(0)) > HEALTH_THRESHOLDS["fiber_min"], 1).otherwise(0)) +
        (F.when(F.coalesce(F.col("protein"), F.lit(0)) > HEALTH_THRESHOLDS["protein_min"], 1).otherwise(0))
    )

    labeled = labeled.withColumn(
        "isHealthy",
        F.when(F.col("healthScore") >= HEALTHY_SCORE_MIN, True).otherwise(False)
    )
    labeled = labeled.withColumn(
        "label",
        F.when(F.col("isHealthy"), 1.0).otherwise(0.0)
    )
    labeled.cache()

    total = labeled.count()
    healthy = labeled.filter(F.col("isHealthy")).count()
    unhealthy = total - healthy
    print(f"  Total labeled: {total} | Healthy: {healthy} | Unhealthy: {unhealthy}")

    return labeled


# ── Step 3: SparkSQL queries (timed for comparison with MongoDB) ─────────

def run_spark_queries(spark: SparkSession, labeled_df):
    """Run SparkSQL queries equivalent to the MongoDB queries, with timing."""
    print("\n" + "=" * 60)
    print("STEP 3: SparkSQL Queries (timed for MongoDB comparison)")
    print("=" * 60)

    labeled_df.createOrReplaceTempView("foods")
    query_results = {}

    queries = {
        "count_by_category": """
            SELECT foodCategory, COUNT(*) as count
            FROM foods
            GROUP BY foodCategory
            ORDER BY count DESC
        """,
        "avg_nutrients_by_category": """
            SELECT foodCategory,
                   ROUND(AVG(calories), 2) as avg_calories,
                   ROUND(AVG(protein), 2) as avg_protein,
                   ROUND(AVG(total_fat), 2) as avg_fat,
                   ROUND(AVG(total_sugars), 2) as avg_sugars
            FROM foods
            GROUP BY foodCategory
            ORDER BY avg_calories DESC
        """,
        "top_10_highest_calorie": """
            SELECT description, foodCategory, calories, protein, total_fat
            FROM foods
            ORDER BY calories DESC
            LIMIT 10
        """,
        "healthy_vs_unhealthy_by_category": """
            SELECT foodCategory,
                   SUM(CASE WHEN isHealthy THEN 1 ELSE 0 END) as healthy_count,
                   SUM(CASE WHEN NOT isHealthy THEN 1 ELSE 0 END) as unhealthy_count,
                   ROUND(AVG(healthScore), 2) as avg_health_score
            FROM foods
            GROUP BY foodCategory
            ORDER BY avg_health_score DESC
        """,
        "cross_collection_comparison": """
            SELECT sourceCollection,
                   COUNT(*) as total,
                   ROUND(AVG(calories), 2) as avg_cal,
                   ROUND(AVG(protein), 2) as avg_protein,
                   ROUND(AVG(total_sugars), 2) as avg_sugars,
                   ROUND(AVG(healthScore), 2) as avg_health_score
            FROM foods
            GROUP BY sourceCollection
        """,
        "high_protein_low_fat": """
            SELECT description, foodCategory, protein, total_fat, calories
            FROM foods
            WHERE protein > 20 AND total_fat < 5
            ORDER BY protein DESC
            LIMIT 10
        """,
    }

    for name, sql in queries.items():
        start = time.time()
        result_df = spark.sql(sql)
        result_df.collect()
        elapsed = time.time() - start
        query_results[name] = {"elapsed_s": round(elapsed, 4)}
        print(f"\n  Query: {name} ({elapsed:.4f}s)")
        result_df.show(10, truncate=40)

    return query_results


# ── Step 4a: Random Forest ML Pipeline ────────────────────────────────────

def train_random_forest(labeled_df):
    """Train a Random Forest classifier to predict healthy vs unhealthy."""
    print("\n" + "=" * 60)
    print("STEP 4: Random Forest Classifier")
    print("=" * 60)

    ml_df = labeled_df.select(FEATURE_COLS + ["label", "description", "foodCategory"])
    for col in FEATURE_COLS:
        ml_df = ml_df.withColumn(col, F.coalesce(F.col(col), F.lit(0.0)))

    ml_df = ml_df.filter(F.col("label").isNotNull())

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        numTrees=100,
        maxDepth=8,
        seed=42,
    )
    pipeline = Pipeline(stages=[assembler, rf])

    train_df, test_df = ml_df.randomSplit([0.8, 0.2], seed=42)
    print(f"  Train: {train_df.count()} | Test: {test_df.count()}")

    start = time.time()
    model = pipeline.fit(train_df)
    train_time = time.time() - start
    print(f"  Training time: {train_time:.2f}s")

    predictions = model.transform(test_df)

    acc_eval = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction"
    )
    auc_eval = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction"
    )

    metrics = {
        "accuracy": round(acc_eval.evaluate(predictions, {acc_eval.metricName: "accuracy"}), 4),
        "precision": round(acc_eval.evaluate(predictions, {acc_eval.metricName: "weightedPrecision"}), 4),
        "recall": round(acc_eval.evaluate(predictions, {acc_eval.metricName: "weightedRecall"}), 4),
        "f1": round(acc_eval.evaluate(predictions, {acc_eval.metricName: "f1"}), 4),
        "auc": round(auc_eval.evaluate(predictions), 4),
        "train_time_s": round(train_time, 2),
        "train_size": train_df.count(),
        "test_size": test_df.count(),
        "num_trees": 100,
        "max_depth": 8,
    }

    print(f"\n  --- Evaluation Metrics ---")
    for k, v in metrics.items():
        print(f"    {k:>20s}: {v}")

    rf_model = model.stages[-1]
    importances = rf_model.featureImportances.toArray()
    print(f"\n  --- Feature Importances ---")
    for feat, imp in sorted(zip(FEATURE_COLS, importances), key=lambda x: -x[1]):
        bar = "#" * int(imp * 40)
        print(f"    {feat:>15s}: {imp:.4f}  {bar}")

    print(f"\n  --- Sample Predictions ---")
    predictions.select(
        "description", "foodCategory", "label", "prediction", "probability"
    ).show(15, truncate=40)

    return model, predictions, metrics


# ── Step 4a: Gradient Boosted Trees ML Pipeline ────────────────────────────────────

def train_gradient_boosted_trees(labeled_df):
    """
    Train a gradient boosted trees classifier to predict healthy vs. unhealthy  food
    """
    print("\n" + "=" * 60)
    print("STEP 4b: Gradient Boosted Trees Classifier")
    print("=" * 60)

    ml_df = labeled_df.select(FEATURE_COLS + ['label', 'description', 'foodCategory'])

    for col in FEATURE_COLS:
        ml_df = ml_df.withColumn(col, F.coalesce(F.col(col), F.lit(0.0)))

    ml_df = ml_df.filter(F.col('label').isNotNull())

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    gbt = GBTClassifier(
        featuresCol="features",
        labelCol="label",
        maxIter=100,
        maxDepth=8,
        stepSize=0.1,
        seed=42,
    )

    pipeline = Pipeline(stages=[assembler,gbt])

    # use same 80/20 split and seed as RandomForest model 
    train_df, test_df = ml_df.randomSplit([0.8, 0.2], seed=42)
    print(f"  Train: {train_df.count()} | Test: {test_df.count()}")

    start = time.time()
    model = pipeline.fit(train_df)
    train_time = time.time() - start
    print(f"  Training time: {train_time:.2f}s")

    # predictions 
    predictions = model.transform(test_df)

    acc_eval = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction"
    )

    # outputs predictions as 2-element vector 
    auc_eval = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction"
    )

    metrics = {
        "accuracy": round(acc_eval.evaluate(predictions, {acc_eval.metricName: "accuracy"}), 4),
        "precision": round(acc_eval.evaluate(predictions, {acc_eval.metricName: "weightedPrecision"}), 4),
        "recall": round(acc_eval.evaluate(predictions, {acc_eval.metricName: "weightedRecall"}), 4),
        "f1": round(acc_eval.evaluate(predictions, {acc_eval.metricName: "f1"}), 4),
        "auc": round(auc_eval.evaluate(predictions), 4),
        "train_time_s": round(train_time, 2),
        "train_size": train_df.count(),
        "test_size": test_df.count(),
        "max_iter": 100,
        "max_depth": 8,
        "step_size": 0.1,
    }

    print(f"\n  --- Evaluation Metrics ---")
    for k, v in metrics.items():
        print(f"    {k:>20s}: {v}")

    gbt_model = model.stages[-1]
    importances = gbt_model.featureImportances.toArray()
    print(f"\n  --- Feature Importances ---")
    for feat, imp in sorted(zip(FEATURE_COLS, importances), key=lambda x: -x[1]):
        bar = "#" * int(imp * 40)
        print(f"    {feat:>15s}: {imp:.4f}  {bar}")
 
    print(f"\n  --- Sample Predictions ---")
    predictions.select(
        "description", "foodCategory", "label", "prediction", "probability"
    ).show(15, truncate=40)
 
    return model, predictions, metrics


# ── Step 5: Save results locally ─────────────────────────────────────────
 
def save_results_locally(
    rf_predictions, rf_metrics,
    gbt_predictions, gbt_metrics,
    query_results,
):
    """Save predictions and metrics for both models to local files."""
    print("\n" + "=" * 60)
    print("STEP 5: Saving results locally")
    print("=" * 60)
 
    os.makedirs(OUTPUT_DIR, exist_ok=True)
 
    # ── Random Forest outputs ──────────────────────────────────────────────
    rf_metrics_path = os.path.join(OUTPUT_DIR, "rf_model_metrics.json")
    with open(rf_metrics_path, "w") as f:
        json.dump(rf_metrics, f, indent=2)
    print(f"  Saved RF metrics      -> {rf_metrics_path}")
 
    rf_pred_path = os.path.join(OUTPUT_DIR, "rf_predictions.csv")
    rf_pred_pdf = rf_predictions.select(
        "description", "foodCategory", "label", "prediction",
        *FEATURE_COLS
    ).toPandas()
    rf_pred_pdf.to_csv(rf_pred_path, index=False)
    print(f"  Saved RF predictions  ({len(rf_pred_pdf)} rows) -> {rf_pred_path}")
 
    # ── GBT outputs ───────────────────────────────────────────────────────
    gbt_metrics_path = os.path.join(OUTPUT_DIR, "gbt_model_metrics.json")
    with open(gbt_metrics_path, "w") as f:
        json.dump(gbt_metrics, f, indent=2)
    print(f"  Saved GBT metrics     -> {gbt_metrics_path}")
 
    gbt_pred_path = os.path.join(OUTPUT_DIR, "gbt_predictions.csv")
    gbt_pred_pdf = gbt_predictions.select(
        "description", "foodCategory", "label", "prediction",
        *FEATURE_COLS
    ).toPandas()
    gbt_pred_pdf.to_csv(gbt_pred_path, index=False)
    print(f"  Saved GBT predictions ({len(gbt_pred_pdf)} rows) -> {gbt_pred_path}")
 
    # ── Combined model comparison ─────────────────────────────────────────
    comparison = {
        "random_forest": rf_metrics,
        "gradient_boosted_trees": gbt_metrics,
    }
    comparison_path = os.path.join(OUTPUT_DIR, "model_comparison.json")
    with open(comparison_path, "w") as f:
        json.dump(comparison, f, indent=2)
    print(f"  Saved model comparison -> {comparison_path}")
 
    print("\n  --- Model Comparison Summary ---")
    header = f"  {'Metric':<20s}  {'Random Forest':>15s}  {'GBT':>15s}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for key in ("accuracy", "precision", "recall", "f1", "auc", "train_time_s"):
        rf_val  = rf_metrics.get(key, "N/A")
        gbt_val = gbt_metrics.get(key, "N/A")
        print(f"  {key:<20s}  {str(rf_val):>15s}  {str(gbt_val):>15s}")
 
    # ── Query timing ──────────────────────────────────────────────────────
    query_path = os.path.join(OUTPUT_DIR, "query_timing.json")
    with open(query_path, "w") as f:
        json.dump(query_results, f, indent=2)
    print(f"\n  Saved query timing    -> {query_path}")
 
    print(f"\n  All local outputs in: {OUTPUT_DIR}/")
 
 
def upload_results_to_gcs():
    """Upload local output/ directory to GCS. Call separately when ready to deploy."""
    from google.oauth2 import service_account
    from google.cloud import storage
 
    key_path = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
    project_id = os.getenv("GCP_PROJECT_ID")
    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = storage.Client(project=project_id, credentials=credentials)
 
    bucket_name = "survey-foods-dds-final-proj"
    bucket = client.bucket(bucket_name)
 
    for fname in os.listdir(OUTPUT_DIR):
        fpath = os.path.join(OUTPUT_DIR, fname)
        if os.path.isfile(fpath):
            blob_path = f"ml_output/{fname}"
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(fpath)
            print(f"  Uploaded {fname} -> gs://{bucket_name}/{blob_path}")
 
 
# ── Main ─────────────────────────────────────────────────────────────────
 
if __name__ == "__main__":
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
     
    # Suppress DAGScheduler broadcast warnings
    log4j = spark.sparkContext._jvm.org.apache.log4j
    log4j.LogManager.getLogger("org.apache.spark.scheduler.DAGScheduler") \
        .setLevel(log4j.Level.ERROR)
    try:
        dfs = load_raw_dataframes(spark)
        labeled_df = label_foods(dfs["all_foods"])
        query_results = run_spark_queries(spark, labeled_df)
 
        rf_model, rf_predictions, rf_metrics = train_random_forest(labeled_df)
        gbt_model, gbt_predictions, gbt_metrics = train_gradient_boosted_trees(labeled_df)
 
        save_results_locally(
            rf_predictions, rf_metrics,
            gbt_predictions, gbt_metrics,
            query_results,
        )
 
        print("\n" + "=" * 60)
        print("PIPELINE COMPLETE")
        print("=" * 60)
        print("  To upload results to GCS, run:")
        print("    python -c \"from spark_ml_pipeline import upload_results_to_gcs; upload_results_to_gcs()\"")
 
    finally:
        spark.stop()