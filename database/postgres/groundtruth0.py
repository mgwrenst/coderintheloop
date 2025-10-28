from postgres_utils import get_conninfo, load_table, run_sql_file
import psycopg

def main():
    conninfo = get_conninfo(dbname="groundtruth0")
    with psycopg.connect(**conninfo) as conn, conn.cursor() as cur:
        tables = [
            ("aksjeeiebok", "database/postgres/schema/aksjeeiebok.sql", "data/postgres/aksjeeiebok.csv", ";", None, None),
            ("politikere", "database/postgres/schema/politikere.sql", "data/postgres/politikere.csv", ";", None, "DMY"),
            ("konkurs", "database/postgres/schema/konkurs.sql", "data/postgres/bankrupt_2020_120925.csv", ",", None, None),
            ("selskap", "database/postgres/schema/selskap.sql", "data/postgres/companies_active_companies_pop_100925_v3.csv", ",", None, None),
            ("person", "database/postgres/schema/person.sql", "data/postgres/persons_active_companies_pop_100925_v3_keep.csv", ",", None, None ),
            ("eierskap", "database/postgres/schema/eierskap.sql", "data/postgres/ownerships_2023_2025.csv", ",", None, None)
        ]
    
    for table in tables:
        load_table(cur, *table)
    
    print("[SUCCESS] Groundtruth0 DB built")