"""
DAG 2: MongoDB aggregation + Spark ML pipeline.
    MongoDB Atlas -> Flatten & Label (aggregation) -> PySpark Random Forest + GBT

Runs the MongoDB aggregation pipeline to flatten nutrients and label
foods as healthy/unhealthy, then trains both a Random Forest classifier
and a Gradient Boosted Trees classifier in PySpark, evaluates both
models, and saves per-model outputs plus a side-by-side comparison.
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
        """
        Load data into Spark, run SparkSQL queries, train Random Forest
        and Gradient Boosted Trees classifiers, save all outputs and a
        side-by-side model comparison.
        """
        from pyspark.sql import SparkSession

        from spark_ml_pipeline import (
            load_raw_dataframes,
            label_foods,
            run_spark_queries,
            train_random_forest,
            train_gradient_boosted_trees,
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

            rf_model, rf_predictions, rf_metrics = train_random_forest(labeled_df)
            gbt_model, gbt_predictions, gbt_metrics = train_gradient_boosted_trees(labeled_df)

            save_results_locally(
                rf_predictions, rf_metrics,
                gbt_predictions, gbt_metrics,
                query_results,
            )
        finally:
            spark.stop()

        return {
            "random_forest": rf_metrics,
            "gradient_boosted_trees": gbt_metrics,
        }

    agg_summary = run_mongo_aggregation()
    metrics = run_spark_pipeline(agg_summary)


spark_ml_dag()