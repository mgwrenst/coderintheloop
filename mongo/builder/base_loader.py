import datetime
from datetime import date, datetime

def pg_row_to_bson(col_names, row):
    """Convert Postgres row to a Mongo document."""
    doc = {}
    for col, val in zip(col_names, row):
        # Convert date -> datetime
        if isinstance(val, date) and not isinstance(val, datetime):
            val = datetime(val.year, val.month, val.day)
        doc[col] = val
    return doc
