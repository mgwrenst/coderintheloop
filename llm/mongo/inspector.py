from pymongo import MongoClient

class MongoInspector:
    def __init__(self, uri, db_name):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]

    def get_schema_overview(self):
        schema = {}

        for name in self.db.list_collection_names():
            coll = self.db[name]
            sample = coll.find_one()
            schema[name] = {
                "sample_document": sample,
                "index_info": coll.index_information()
            }

        return schema
