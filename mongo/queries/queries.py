import os
import sys
import json
from pathlib import Path
from pymongo import MongoClient
from pprint import pprint

def main():
    # --- MongoDB connection ---
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    DB_NAME = os.getenv("DB_NAME", "groundtruthsmall")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # --- Select benchmark file (raw vs structured) ---
    use_structured = ("--structured" in sys.argv) or (os.getenv("BENCHMARK", "").lower() == "structured")
    benchmarks_dir = Path(__file__).parent.parent / "benchmarks"
    benchmark_file = benchmarks_dir / ("benchmark_structured.json" if use_structured else "benchmark.json")

    if not benchmark_file.exists():
        print(f"[ERROR] Benchmark file not found: {benchmark_file}")
        print("Add it under mongo/benchmarks or remove --structured flag.")
        sys.exit(1)

    print(f"[INFO] Using benchmark file: {benchmark_file.name}")
    # --- Load queries ---
    with open(benchmark_file, "r", encoding="utf-8") as f:
        queries = json.load(f)

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
    filter_ = query_def.get("filter", {})
    projection = query_def.get("projection", None)

    print(f"\n▶ Running query '{query_name}' on collection '{query_def['collection']}'...\n")

    # --- Execute query ---
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
