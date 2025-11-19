
class QueryValidator:
    SAFE_OPERATIONS = {"find"}
    FORBIDDEN_KEYS = {"$where", "$function"}

    def validate(self, query):
        if query["operation"] not in self.SAFE_OPERATIONS:
            raise ValueError("Operation not allowed.")

        def check(obj):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if key in self.FORBIDDEN_KEYS:
                        raise ValueError(f"Forbidden operator: {key}")
                    check(value)
            elif isinstance(obj, list):
                for v in obj:
                    check(v)

        check(query)
        return True
