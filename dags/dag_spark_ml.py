"""
DAG 2: MongoDB aggregation + Spark ML pipeline.
    MongoDB Atlas -> Flatten & Label (aggregation) -> PySpark Random Forest

Runs the MongoDB aggregation pipeline to flatten nutrients and label
foods as healthy/unhealthy, then trains a Random Forest classifier
in PySpark and evaluates model performance.
"""

import sys
import datetime
from pathlib import Path

from airflow.sdk import dag, task

PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


@dag(
    dag_id="spark_ml_pipeline",
    schedule=None,
    start_date=datetime.datetime(2026, 3, 1),
    catchup=False,
    tags=["usda", "mongodb", "spark", "ml"],
    default_args={
        "owner": "dds-group",
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=2),
    },
)
def spark_ml_dag():

    @task()
    def run_mongo_aggregation() -> dict:
        """Flatten foodNutrients and label healthy/unhealthy in MongoDB."""
        from aggregate_and_query import get_db, flatten_and_label_all_foods

        client, db = get_db()
        results = flatten_and_label_all_foods(db, dry_run=False)
        healthy = sum(1 for r in results if r.get("isHealthy"))
        unhealthy = len(results) - healthy
        client.close()
        return {"total": len(results), "healthy": healthy, "unhealthy": unhealthy}

    @task()
    def run_spark_pipeline(agg_summary: dict) -> dict:
        """Load data into Spark, run SparkSQL queries, train Random Forest."""
        from pyspark.sql import SparkSession

        from spark_ml_pipeline import (
            load_raw_dataframes,
            label_foods,
            run_spark_queries,
            train_random_forest,
            save_results_locally,
        )

        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("USDA_Food_Classification") \
            .config("spark.driver.memory", "4g") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        try:
            dfs = load_raw_dataframes(spark)
            labeled_df = label_foods(dfs["all_foods"])
            query_results = run_spark_queries(spark, labeled_df)
            model, predictions, metrics = train_random_forest(labeled_df)
            save_results_locally(predictions, metrics, query_results)
        finally:
            spark.stop()

        return metrics

    agg_summary = run_mongo_aggregation()
    metrics = run_spark_pipeline(agg_summary)


spark_ml_dag()
