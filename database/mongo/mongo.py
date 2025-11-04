import os
import psycopg
from dotenv import load_dotenv
from pymongo import MongoClient
from mongo_utils import transfer_table, get_pg_conninfo

load_dotenv()

def main():
    pg_conninfo = get_pg_conninfo()
    with psycopg.connect(**pg_conninfo) as pg_conn:
        # Connect to MongoDB
        mongo_client = MongoClient("mongodb://localhost:27017")
        mongo_db = mongo_client["groundtruthsmall"]

        # Clear collections before loading
        for coll in ["selskap", "eierskap", "politikere", "aksjeeiebok"]:
            mongo_db[coll].drop()
        print("Old collections cleared.")

        # Transfer tables 
        transfer_table(pg_conn, mongo_db, "eierskap", "eierskap")
        transfer_table(pg_conn, mongo_db, "politikere", "politikere")
        transfer_table(pg_conn, mongo_db, "aksjeeiebok", "aksjeeiebok")
        transfer_table(pg_conn, mongo_db, "alleselskaper", "selskap")

    print("Data transfer complete.")

if __name__ == "__main__":
    main()
