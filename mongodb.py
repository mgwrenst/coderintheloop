import pymongo
from pymongo import MongoClient

cluster = MongoClient("mongodb+srv://mongo:mongoDB@cluster0.3n9zbj6.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")

db = cluster["test"]
collection = db["test"]


# =========================
# 1️⃣ Insert Example Docs
# =========================
"""
# Insert a couple of sample documents
post1 = {"_id": 1, "name": "Tim", "score": 5}
post2 = {"_id": 2, "name": "Bill", "score": 10}
collection.insert_many([post1, post2])
print("Inserted example documents.")
"""


# =========================
# 2️⃣ Find All Documents
# =========================
"""
results = collection.find({})
for doc in results:
    print(doc)
"""

# =========================
# 3️⃣ Find by Field
# =========================
"""
# Query by name
results = collection.find({"name": "Tim"})
for doc in results:
    print(doc)
"""

# =========================
# 4️⃣ Update a Document
# =========================
"""
# Increase Tim's score by 1
update_result = collection.update_one({"name": "Tim"}, {"$inc": {"score": 1}})
print(f"Matched: {update_result.matched_count}, Modified: {update_result.modified_count}")
"""

# =========================
# 5️⃣ Delete a Document
# =========================
"""
# Delete the document where name == 'Bill'
delete_result = collection.delete_one({"name": "Bill"})
print(f"Deleted count: {delete_result.deleted_count}")
"""

# =========================
# 6️⃣ Count Documents
# =========================
"""
count = collection.count_documents({})
print(f"Total documents: {count}")
"""

# =========================
# 7️⃣  Find with Operators
# =========================
"""
# Find documents where score is greater than 5
results = collection.find({"score": {"$gt": 5}})
for doc in results:
    print(doc)
"""

# =========================
# 8️⃣  Projection (Select Fields)
# =========================
"""
# Return only name and score fields, hide the _id
results = collection.find({}, {"_id": 0, "name": 1, "score": 1})
for doc in results:
    print(doc)
"""

# =========================
# 9️⃣  Sorting Results
# =========================
"""
# Sort documents by score descending
results = collection.find().sort("score", -1)
for doc in results:
    print(doc)
"""

# =========================
# 🔟  Limit & Skip
# =========================
"""
# Skip the first document and limit to 2 results
results = collection.find().skip(1).limit(2)
for doc in results:
    print(doc)
"""

# =========================
# 11️⃣  Upsert (Update or Insert)
# =========================
"""
# Update Tim's score to 20 or insert if Tim doesn't exist
result = collection.update_one(
    {"name": "Tim"},
    {"$set": {"score": 20}},
    upsert=True
)
print(f"Matched: {result.matched_count}, Modified: {result.modified_count}, Upserted ID: {result.upserted_id}")
"""

# =========================
# 12️⃣  Bulk Write
# =========================
"""
from pymongo import InsertOne, DeleteOne, UpdateOne

requests = [
    InsertOne({"name": "Alice", "score": 7}),
    UpdateOne({"name": "Bill"}, {"$set": {"score": 15}}),
    DeleteOne({"name": "Tim"})
]
result = collection.bulk_write(requests)
print("Inserted:", result.inserted_count)
print("Updated:", result.modified_count)
print("Deleted:", result.deleted_count)
"""

# =========================
# 13️⃣  Aggregation Pipeline
# =========================
"""
# Average score grouped by name
pipeline = [
    {"$group": {"_id": "$name", "avgScore": {"$avg": "$score"}}},
    {"$sort": {"avgScore": -1}}
]
for doc in collection.aggregate(pipeline):
    print(doc)
"""

# =========================
# 14️⃣  Create an Index
# =========================
"""
# Create an ascending index on 'score'
index_name = collection.create_index([("score", 1)])
print(f"Created index: {index_name}")
"""
