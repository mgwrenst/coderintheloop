from postgres_utils import get_conninfo, load_table, run_sql_file
import psycopg

def main():
    conninfo = get_conninfo(dbname="groundtruthsmall")
    with psycopg.connect(**conninfo) as conn, conn.cursor() as cur:
        tables = [
            ("aksjeeiebok", "database/postgres/schema/aksjeeiebok.sql", r"C:\Users\wren9\Downloads\GT0small\files\aksjeeiebok.csv", ";", None, None),
            ("politikere", "database/postgres/schema/politikere.sql", r"C:\Users\wren9\Downloads\GT0small\files\politikere.csv", ";", None, "DMY"),
            ("konkurs", "database/postgres/schema/konkurs.sql", r"C:\Users\wren9\Downloads\GT0small\files\bankrupt_2020_120925.csv", ",", None, None),
            ("selskap", "database/postgres/schema/selskap.sql", r"C:\Users\wren9\Downloads\GT0small\files\companies_active_companies_pop_100925_v3.csv", ",", None, None),
            ("person", "database/postgres/schema/person.sql", r"C:\Users\wren9\Downloads\GT0small\files\persons_active_companies_pop_100925_v3_keep.csv", ",", None, None ),
            ("eierskap", "database/postgres/schema/eierskap.sql", r"C:\Users\wren9\Downloads\GT0small\files\ownerships_2023_2025.csv", ",", None, None)
        ]

        for table in tables:
            load_table(cur, *table)

        #run_sql_file(cur, "schema/views.sql")

        print("[SUCCESS] Groundtruthsmall DB built.")

if __name__ == "__main__":
    main()