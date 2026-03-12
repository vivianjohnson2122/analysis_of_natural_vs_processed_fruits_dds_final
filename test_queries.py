"""Run and print results from aggregate_and_query.py to verify queries work."""

from dotenv import load_dotenv
from aggregate_and_query import (
    get_db,
    get_all_foods,
    get_foods_by_category,
    count_foods_by_category,
    avg_nutrients_by_category,
    top_foods_by_nutrient,
    cross_collection_comparison,
    NUTRIENT_IDS,
    RAW_COLLECTIONS,
)

load_dotenv()


def main():
    client, db = get_db()

    print("=" * 60)
    print("TEST: Queries on Raw Collections")
    print("=" * 60)

    for coll in RAW_COLLECTIONS:
        print(f"\n--- {coll} ---")

        foods = get_all_foods(db, coll, limit=3)
        for f in foods:
            print(f"    {f.get('description', 'N/A')[:60]}")

        cats = count_foods_by_category(db, coll)
        print(f"    Top 3 categories: {[(c['_id'], c['count']) for c in cats[:3]]}")

    print(f"\n--- Top 5 highest-calorie foods (survey) ---")
    top_cal = top_foods_by_nutrient(db, "survey-food-data-collection", NUTRIENT_IDS["calories"], 5)
    for f in top_cal:
        print(f"    {f['description'][:50]:50s} | {f['value']} {f['unit']}")

    print(f"\n--- Top 5 highest-protein foods (legacy) ---")
    top_prot = top_foods_by_nutrient(db, "legacy-food-data-collection", NUTRIENT_IDS["protein"], 5)
    for f in top_prot:
        print(f"    {f['description'][:50]:50s} | {f['value']} {f['unit']}")

    print(f"\n--- Fruits and Fruit Juices (legacy) ---")
    fruits = get_foods_by_category(db, "legacy-food-data-collection", "Fruits and Fruit Juices")
    print(f"    Found {len(fruits)} items")
    for f in fruits[:5]:
        print(f"    {f['description'][:60]}")

    print(f"\n--- Cross-collection comparison ---")
    comparison = cross_collection_comparison(db)
    for r in comparison:
        print(f"    {r['collection'][:30]:30s} | {r['_id']:30s} | avg={r['avgValue']:.2f}")

    print(f"\n--- Avg nutrients by category (foundation) ---")
    cats = avg_nutrients_by_category(db, "foundation-food-data-collection")
    for c in cats[:5]:
        print(f"    {c['_id']}")
        for n in c["nutrients"]:
            print(f"      {n['name']}: {n['avg']}")

    client.close()
    print("\nAll tests passed.")


if __name__ == "__main__":
    main()
