from pymongo import MongoClient
import os
import json

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "groundtruth0")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]


# --- load queries from JSON ---
with open("database/mongo/benchmark.json", "r", encoding="utf-8") as f:
    queries = json.load(f)

# --- choose query ---
print("Available queries:")
for i, name in enumerate(queries.keys(), 1):
    print(f"{i}. {name}")

choice = input("\nEnter query number or name: ").strip()

# allow both numeric or string selection
if choice.isdigit():
    query_name = list(queries.keys())[int(choice) - 1]
else:
    query_name = choice

if query_name not in queries:
    raise ValueError(f"Unknown query name: {query_name}")

query_def = queries[query_name]

print(f"\n▶ Running query: {query_name}\n")

# --- run query ---
collection = db[query_def["collection"]]
filter_ = query_def.get("filter", {})
projection = query_def.get("projection", None)

cursor = collection.find(filter_, projection)

for doc in cursor:
    print(doc)