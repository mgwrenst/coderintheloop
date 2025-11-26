import sys
import psycopg 
from psycopg.rows import dict_row
from tabulate import tabulate
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from main import get_connection_info


def load_queries():
    queries = {}
    queries_file = Path(__file__).parent / "queries.sql"
    
    with open(queries_file, encoding="utf-8") as f:
        content = f.read()
    
    current_name = None
    current_query = []
    
    for line in content.split('\n'):
        if line.startswith('-- '):
            if current_name and current_query:
                queries[current_name] = '\n'.join(current_query).strip()
            current_name = line[3:].strip()
            current_query = []
        elif current_name:
            current_query.append(line)
    
    if current_name and current_query:
        queries[current_name] = '\n'.join(current_query).strip()
    
    return queries

def run_query(name):
    queries = load_queries()
    query = queries.get(name)
    
    if not query:
        print(f"Unknown query '{name}'")
        print(f"Available queries: {', '.join(queries.keys())}")
        return 
    
    conninfo = get_connection_info()
    with psycopg.connect(**conninfo) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            rows = cur.fetchall()
    
    if rows:
        print(tabulate(rows, headers="keys", tablefmt="psql"))
    else:
        print("No results.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        queries = load_queries()
        print("Usage: python queries/queries.py <query_name>")
        print(f"Available queries: {', '.join(queries.keys())}")
        sys.exit(1)
    
    query_name = sys.argv[1]
    run_query(query_name)
     