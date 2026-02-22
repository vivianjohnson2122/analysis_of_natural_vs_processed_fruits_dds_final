from aggregate_and_query import *
import os
from dotenv import load_dotenv

load_dotenv()

# CONFIGURATION
MONGO_ATLAS_USERNAME = os.getenv("MONGODB_ATLAS_USERNAME")
MONGO_ATLAS_PASSWORD = os.getenv("MONGODB_ATLAS_PASSWORD")
MONGO_CLUSTER_URI = "test-cluster.kay1d48.mongodb.net"
DB_NAME = "msds697"
MONGO_URI = f"mongodb+srv://{MONGO_ATLAS_USERNAME}:{MONGO_ATLAS_PASSWORD}@{MONGO_CLUSTER_URI}/{DB_NAME}?retryWrites=true&w=majority"


print("Testing get_all_fruits...")
result1 = get_all_fruits(MONGO_URI, DB_NAME, "fruit-data-collection")
print(f"Found {len(result1)} fruits")
print(f"First result: {result1[0] if result1 else 'None'}\n")

print("Testing get_fruits_by_order...")
result2 = get_fruits_by_order(MONGO_URI, DB_NAME, "fruit-data-collection", "Rosales")
print(f"Found {len(result2)} fruits in order Rosales")
for item in result2:
    print(f"  {item.get('name')}: {item.get('family')}")
print()

print("Testing get_snacks_with_no_additives...")
result4 = get_snacks_with_no_additives(MONGO_URI, DB_NAME, "snack-data-collection")
print(f"Found {len(result4)} snacks with no additives")
print(f"First result: {result4[0] if result4 else 'None'}\n")


print("Testing get_families_with_multiple_fruits...")
result7 = get_families_with_multiple_fruits(MONGO_URI, DB_NAME, "fruit-family-aggregates")
print(f"Found {len(result7)} families with more than 1 fruit")
for item in result7:
    print(f"  {item.get('_id')}: {item.get('count')} fruits - {item.get('fruits')}")
print()

print("All tests complete!")