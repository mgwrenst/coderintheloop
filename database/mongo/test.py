import os
import json
from pymongo import MongoClient
from pprint import pprint

def main():
    # --- MongoDB connection ---
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    DB_NAME = os.getenv("DB_NAME", "groundtruthsmall")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # --- Load queries ---
    with open("database/mongo/benchmark_structured.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    # --- Support for both dict and list-style benchmark files ---
    if isinstance(data, list):
        # new benchmark.json format (with pipeline, description, etc.)
        queries = {q["name"]: q for q in data}
    else:
        # old format (simple dict of queries)
        queries = data

    # --- Show available queries ---
    print("\nAvailable queries:")
    for i, name in enumerate(queries.keys(), 1):
        print(f"{i}. {name}")

    # --- Select query ---
    choice = input("\nEnter query number or name: ").strip()
    query_name = list(queries.keys())[int(choice) - 1] if choice.isdigit() else choice

    if query_name not in queries:
        print(f"[ERROR] Unknown query name: {query_name}")
        return

    query_def = queries[query_name]
    collection = db[query_def["collection"]]

    print(f"\n▶ Running query '{query_name}' on collection '{query_def['collection']}'...\n")

    # --- Detect query type ---
    if "pipeline" in query_def:
        # Aggregation-style benchmark
        results = collection.aggregate(query_def["pipeline"])
    else:
        # Simple find-style benchmark
        filter_ = query_def.get("filter", {})
        projection = query_def.get("projection", None)
        results = collection.find(filter_, projection)

    count = 0
    for doc in results:
        pprint(doc)
        print("-" * 60)
        count += 1

    if count == 0:
        print("[INFO] No documents matched the query.")
    else:
        print(f"[DONE] Retrieved {count} documents.")

    client.close()

if __name__ == "__main__":
    main()
