from pymongo import MongoClient

def execute_query(db, query):
    col = db[query["collection"]]
    return list(col.find(query["filter"], query.get("projection")))

def run():
    # Generate query (as above)
    # ...
    client = MongoClient("mongodb://localhost:27017")
    db = client["yourdb"]

    results = execute_query(db, query)
    print(results)
