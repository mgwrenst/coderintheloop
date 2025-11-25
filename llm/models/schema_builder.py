import json

class SchemaBuilder:
    @staticmethod
    def build_human_readable(schema_dict):
        return json.dumps(schema_dict, indent=4, ensure_ascii=False)
