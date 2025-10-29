from pathlib import Path
from postgres_utils import get_conninfo, load_table, run_sql_file
import psycopg

def main():

    base = Path(r"C:\Users\wren9\Downloads\GT0small\GT0small\files")  # CSV files location
    schema_base = Path("database/postgres/schema")  # location of schema folder

    tables = [
        ("aksjeeiebok", schema_base / "aksjeeiebok.sql", base / "aksjeeiebok.csv", ";", None, None),
        ("politikere",  schema_base / "politikere.sql",  base / "politikere.csv",  ";", None, "SQL, DMY"),
        ("konkurs",     schema_base / "konkurs.sql",     base / "bankrupt_2020_120925.csv", ",", ["nr"], None),
        ("selskap",     schema_base / "selskap.sql",     base / "companies_active_companies_pop_100925_v3.csv", ",", ["nr"], None),
        ("person",      schema_base / "person.sql",      base / "persons_active_companies_pop_100925_v3_keep.csv", ",", [
            "nr", "person_birth_day", "person_birth_month", "person_surrogate_key", "person_surrugate_key",
            "person_data_origin_ids", "person_disambiguate_uuid", "person_person_location_type_key",
            "person_national_identification_number", "person_national_identification_schema",
            "person_company_role_meta_role_elector_id", "person_company_role_meta_role_responsibility",
            "person_company_role_meta_role_responsibility_percentage", "person_company_role_external_url",
            "person_company_role_resigned_flag", "person_company_role_surrogate_key",
            "person_company_role_surrugate_key", "person_company_role_data_source_uuid",
            "person_street_name", "person_street_letter", "person_street_number",
            "person_person_master_uuid", "person_composite_business_key", "company_org_nr_schema",
            "person_company_role_business_key", "personSelskapRollePersonUUID", "personSelskapRolleSelskapUUID",
            "personSelskapRolleSelskapRolleUUID", "personSelskapRolleRegistrertTid", "personSelskapRolleOppdatertTid"
        ], None),
        ("eierskap",    schema_base / "eierskap.sql",    base / "ownerships_2023_2025.csv", ",", [
            "nr", "shareholder_person_birth_month", "shareholder_person_birth_day",
            "company_share_ownership_ownership_lower", "company_share_ownership_ownership_upper",
            "company_share_ownership_voting_ownership_lower", "company_share_ownership_voting_ownership_upper",
            "company_share_ownership_share_issuer_company_uuid", "company_share_ownership_shareholder_person_uuid",
            "company_share_ownership_shareholder_company_uuid"
        ], None),
    ]

    conninfo = get_conninfo(dbname="groundtruthsmall")
    with psycopg.connect(**conninfo) as conn, conn.cursor() as cur:
    
        for name, schema_path, csv_path, delim, drops, dstyle in tables:
            try:
                cur.execute("SAVEPOINT before_load;")
                load_table(cur, name, str(schema_path), str(csv_path),
                           delimiter=delim, drop_cols=drops, date_style=dstyle)
            except Exception as e:
                print(f"[ERROR] {name} failed: {e}")
                cur.execute("ROLLBACK TO SAVEPOINT before_load;")

        
        # 🧩 POST-LOAD CLEANUP AND VIEW CREATION
        print("[INFO] Removing overlapping companies (present in both selskap and konkurs)...")
        cur.execute("""
        DELETE FROM selskap
        WHERE UUID IN (SELECT UUID FROM konkurs);
        """)
        print("[OK] Overlapping companies removed.")

        print("[INFO] Creating view 'alleSelskaper' (combined active + bankrupt)...")
        cur.execute("DROP VIEW IF EXISTS alleSelskaper;")
        cur.execute("""
        CREATE VIEW alleSelskaper AS
        SELECT *
        FROM selskap
        UNION ALL
        SELECT *
        FROM konkurs;
        """)
        conn.commit()
        print("[OK] View 'alleSelskaper' created successfully.")


        print("[INFO] Creating indexes...")
        run_sql_file(cur, schema_base / "indexes.sql")
        conn.commit()
        print("[OK] Index creation complete.")


    print("[SUCCESS] Groundtruthsmall DB built.")


if __name__ == "__main__":
    main()
