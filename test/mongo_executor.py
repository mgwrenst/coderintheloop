import pymongo

# Forbidden operators for safety (LLM must not execute JS)
FORBIDDEN_KEYS = {"$where", "$function", "$accumulator"}

def _validate_no_forbidden_operators(obj):
    """Recursively ensure no forbidden Mongo operators appear."""
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in FORBIDDEN_KEYS:
                raise ValueError(f"Forbidden Mongo operator used: {key}")
            _validate_no_forbidden_operators(value)
    elif isinstance(obj, list):
        for item in obj:
            _validate_no_forbidden_operators(item)


def execute_query(db, query: dict):
    """
    Executes an LLM-generated MongoDB query safely.

    query format:
    {
      "collection": "selskap",
      "operation": "find" | "aggregate",
      ...
    }
    """

    # Basic validation
    if "collection" not in query:
        raise ValueError("Missing required field: 'collection'")

    if "operation" not in query:
        raise ValueError("Missing required field: 'operation'")

    collection_name = query["collection"]
    operation = query["operation"]

    col = db[collection_name]

    # Validate operators for safety
    _validate_no_forbidden_operators(query)

    # Handle FIND queries
    if operation == "find":
        filter_ = query.get("filter", {})
        projection = query.get("projection", None)

        cursor = col.find(filter_, projection)

        if "limit" in query:
            cursor = cursor.limit(int(query["limit"]))

        return list(cursor)

    # Handle AGGREGATE queries
    elif operation == "aggregate":
        pipeline = query.get("pipeline", [])
        if not isinstance(pipeline, list):
            raise ValueError("Pipeline must be a list")

        return list(col.aggregate(pipeline))

    else:
        raise ValueError(f"Unsupported Mongo operation: {operation}")
