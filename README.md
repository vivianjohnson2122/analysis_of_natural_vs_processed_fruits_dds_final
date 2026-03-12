# Analysis of Natural vs Processed Foods — DDS Final Project

Automated data pipeline that ingests USDA food data, labels foods as healthy or unhealthy using nutritional thresholds, and trains a Random Forest classifier to predict food health labels.

## Architecture

```
USDA FoodData Central API
        │
        ▼
   [DAG 1: Ingest]
   Fetch JSON → GCS Buckets → MongoDB Atlas (3 collections)
        │
        ▼
   [MongoDB Aggregation Pipeline]
   $unwind foodNutrients → pivot key nutrients →
   score & label healthy/unhealthy → $out to new collections
        │
        ▼
   [DAG 2: Spark ML]
   Load DataFrames from MongoDB → SparkSQL queries →
   Feature engineering → Random Forest Classifier → Evaluate
        │
        ▼
   [DAG 3: Save Results]
   Model metrics + predictions + query timing → GCS
```

## Data Sources

Three datasets from the USDA FoodData Central API:

| Collection | Documents | Description |
|---|---|---|
| survey-food-data-collection | ~10,864 | Food and Nutrient Database for Dietary Studies (FNDDS) |
| legacy-food-data-collection | ~7,793 | Historic USDA National Nutrition Database |
| foundation-food-data-collection | ~365 | Minimally processed commodity foods |

## Health Labeling Criteria

Foods are scored on 6 criteria (1 point each). Score >= 4 = healthy:

- Calories < 300 kcal
- Total sugars < 10g
- Sodium < 400mg
- Saturated fat < 5g
- Fiber > 3g
- Protein > 5g

## ML Model

- **Algorithm**: Random Forest Classifier (100 trees, max depth 8)
- **Features**: calories, protein, total_fat, total_sugars, sodium, fiber, saturated_fat, cholesterol
- **Results**: ~96% accuracy, ~99% AUC

## Setup

```bash
conda activate dds-spring-2026
pip install -r requirements.txt
```

Create a `.env` file with:

```
GCP_SERVICE_ACCOUNT_KEY=/path/to/service-account-key.json
GCP_PROJECT_ID=your-project-id
MONGODB_ATLAS_USERNAME=your-username
MONGODB_ATLAS_PASSWORD=your-password
MONGODB_CLUSTER_HOST=your-cluster.mongodb.net
MONGODB_DB_NAME=msds697
```

## Usage

```bash
# Run MongoDB aggregations (dry run - read only)
python aggregate_and_query.py

# Run Spark ML pipeline (reads from Atlas, saves locally)
python spark_ml_pipeline.py

# Test queries
python test_queries.py
```

## Project Structure

```
├── aggregate_and_query.py     # MongoDB aggregation pipeline + queries
├── spark_ml_pipeline.py       # PySpark ML (Random Forest) + SparkSQL
├── store_data_in_gcs.py       # USDA API → GCS ingestion
├── load_into_mongodb.py       # GCS → MongoDB Atlas loading
├── test_queries.py            # Query verification script
├── dags/
│   ├── dag_ingest.py          # Airflow DAG 1: API → GCS → MongoDB
│   ├── dag_spark_ml.py        # Airflow DAG 2: Aggregation → Spark ML
│   └── dag_save_results.py    # Airflow DAG 3: Results → GCS
├── output/                    # Local ML outputs (gitignored)
├── requirements.txt
└── .env                       # Credentials (gitignored)
```

## Team

MSDS 697 — Distributed Data Systems, Spring 2026
