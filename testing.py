import json
import psycopg
import pymongo
from pymongo import MongoClient
import json


conn = psycopg.connect(dbname="politikk", user="postgres", password="password", host="localhost", port=5432)
cur = conn.cursor()
cur.execute("""SELECT * FROM politikere""")

# Fetch column names for keys
cols = [desc[0] for desc in cur.description]

with open("mytable.json", "w", encoding="utf-8") as f:
    for row in cur:
        doc = dict(zip(cols, row))
        f.write(json.dumps(doc, default=str) + "\n")  # default=str handles dates



""" 
from pymongo import MongoClient
client = MongoClient("your_connection_string")
coll = client.mymongo.mytable
print(coll.count_documents({}))
print(coll.find_one())
 """