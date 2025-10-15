import pymongo
import os
import json
from pymongo import MongoClient
from decimal import Decimal

# --- helper to fix Decimals recursively ---
def fix_decimals(obj):
    if isinstance(obj, dict):
        return {k: fix_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [fix_decimals(v) for v in obj]
    elif isinstance(obj, Decimal):
        return float(obj)  
    else:
        return obj

# --- MongoDB connection ---
client = MongoClient("mongodb://localhost:27017")
db = client["groundtruth0"]

input_dir = "json_exports"

# Loop over all JSON files
for filename in os.listdir(input_dir):
    if filename.endswith(".json"):
        collection_name = filename.replace(".json", "")
        collection = db[collection_name]

        json_file = os.path.join(input_dir, filename)
        print(f"Importing {filename} into collection '{collection_name}'")

        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Fix decimals before insert
        if isinstance(data, list):
            cleaned = [fix_decimals(doc) for doc in data]
            if cleaned:
                collection.insert_many(cleaned)
        else:
            collection.insert_one(fix_decimals(data))

print("✅ All JSON files imported into MongoDB (Decimals converted)!")
