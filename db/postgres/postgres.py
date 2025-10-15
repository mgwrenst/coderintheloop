import os
from dotenv import load_dotenv
import psycopg

# Load environment variables from .env file
load_dotenv()

def get_conninfo_dict():
    # Build a dict for psycopg.connect(**kwargs)
    keys = [
        ("PGHOST", "host"),
        ("PGPORT", "port"),
        ("PGUSER", "user"),
        ("PGPASSWORD", "password"),
        ("PGDATABASE", "dbname"),
    ]
    return {pg_key: os.environ.get(env_key) for env_key, pg_key in keys if os.environ.get(env_key)}

def create_and_load_table(cur, table_sql, truncate_sql, filename, copy_sql, progress_msg):
    print(f"[INFO] {progress_msg} ...")
    cur.execute(table_sql)
    cur.execute(truncate_sql)
    with open(filename, "r", encoding="utf-8") as f:
        with cur.copy(copy_sql) as copy:
            for line in f:
                copy.write(line)
    print(f"[OK] {progress_msg} loaded.")

def drop_columns(cur, table, columns, progress_msg):
    print(f"[INFO] Dropping columns from {table}: {', '.join(columns)} ...")
    drops = ',\n'.join([f"DROP COLUMN IF EXISTS {col}" for col in columns])
    cur.execute(f"ALTER TABLE {table} {drops};")
    print(f"[OK] Columns dropped from {table}.")

def main():
    conninfo_dict = get_conninfo_dict()
    with psycopg.connect(**conninfo_dict) as conn:
        with conn.cursor() as cur:

            # --- TABLE aksjeeiebok ---
            create_and_load_table(
                cur,
                """
                CREATE TABLE IF NOT EXISTS aksjeeiebok (
                    orgNr INTEGER,
                    selskap TEXT,
                    aksjeklasse VARCHAR(30),
                    aksjonærNavn TEXT,
                    aksjonærNr VARCHAR(11),
                    poststed VARCHAR(60),
                    landkode VARCHAR(10),
                    antallAksjer BIGINT,
                    antallAksjerSelskap BIGINT,
                    år SMALLINT
                );
                """,
                "TRUNCATE aksjeeiebok RESTART IDENTITY;",
                "C:/Users/wren9/git/coderintheloop/data/postgres/aksjeeiebok.csv",
                "COPY aksjeeiebok FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ';')",
                "Loading aksjeeiebok"
            )

            # --- TABLE politikere ---
            print("[INFO] Setting datestyle for politikere ...")
            cur.execute("SET datestyle = DMY;")
            print("[OK] Datestyle set.")

            create_and_load_table(
                cur,
                """
                CREATE TABLE IF NOT EXISTS politikere (
                    navn VARCHAR(60),
                    parti TEXT,
                    kommuneNr SMALLINT,
                    kommune VARCHAR(60),
                    fødselsdato DATE,
                    listeplass SMALLINT,
                    stemmetillegg VARCHAR(10),
                    personstemmer INTEGER,
                    slengere INTEGER,
                    endeligRangering SMALLINT,
                    innvalgt VARCHAR(10)
                );
                """,
                "TRUNCATE politikere RESTART IDENTITY;",
                "C:/Users/wren9/git/coderintheloop/data/postgres/politikere.csv",
                "COPY politikere FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ';')",
                "Loading politikere"
            )

            # --- TABLE konkurs ---
            create_and_load_table(
                cur,
                """
                CREATE TABLE IF NOT EXISTS konkurs (
                    nr INTEGER,
                    navn TEXT,
                    UUID VARCHAR(50),
                    OrgNr INTEGER,
                    konkursFlagg SMALLINT,
                    likvidasjonFlagg SMALLINT,
                    naceKode REAL,
                    organisasjonstype TEXT,
                    oppløstDato DATE,
                    etablertDato DATE
                );
                """,
                "TRUNCATE konkurs RESTART IDENTITY;",
                "C:/Users/wren9/git/coderintheloop/data/postgres/bankrupt_2020_120925.csv",
                "COPY konkurs FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',')",
                "Loading konkurs"
            )
            drop_columns(cur, "konkurs", ["nr"], "konkurs")

            # --- TABLE selskap ---
            create_and_load_table(
                cur,
                """
                CREATE TABLE IF NOT EXISTS selskap (
                    nr INTEGER,
                    navn TEXT,
                    UUID VARCHAR(50),
                    orgNr INTEGER,
                    konkursFlagg SMALLINT,
                    likvidasjonFlagg SMALLINT,
                    naceKode REAL,
                    organisasjonstype TEXT,
                    oppløstDato DATE,
                    etablertDato DATE
                );
                """,
                "TRUNCATE selskap RESTART IDENTITY;",
                "C:/Users/wren9/git/coderintheloop/data/postgres/companies_active_companies_pop_100925_v3.csv",
                "COPY selskap FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',')",
                "Loading selskap"
            )
            drop_columns(cur, "selskap", ["nr"], "selskap")

            print("[INFO] Removing overlap between selskap and konkurs ...")
            cur.execute("""
                DELETE FROM selskap
                WHERE UUID IN (SELECT UUID FROM konkurs);
            """)
            print("[OK] Overlap removed.")

            print("[INFO] Creating view alleSelskaper ...")
            cur.execute("""
                CREATE OR REPLACE VIEW alleSelskaper AS
                SELECT * FROM selskap
                UNION
                SELECT * FROM konkurs;
            """)
            print("[OK] View alleSelskaper created.")

            # --- TABLE person ---
            create_and_load_table(
                cur,
                """
                CREATE TABLE IF NOT EXISTS person (
                    nr INTEGER,
                    UUID VARCHAR(50),
                    person_birth_day REAL,
                    navn TEXT,
                    fødselsdato DATE,
                    fødselsår REAL,
                    person_birth_month REAL,
                    kjønnUUID VARCHAR(50),
                    postnummer SMALLINT,
                    person_street_name TEXT,
                    land TEXT,
                    postkode TEXT,
                    person_street_letter TEXT,
                    person_street_number TEXT,
                    person_surrogate_key TEXT,
                    person_surrugate_key SMALLINT,
                    adresse TEXT,
                    person_data_origin_ids TEXT,
                    landKode VARCHAR(10),
                    registrertTid TIMESTAMP,
                    oppdatertTid TIMESTAMP,
                    person_disambiguate_uuid VARCHAR(50),
                    kommuneNr SMALLINT,
                    kommuneNavn TEXT,
                    person_person_master_uuid VARCHAR(50),
                    person_composite_business_key TEXT,
                    person_person_location_type_key TEXT,
                    person_national_identification_number VARCHAR(50),
                    person_national_identification_schema VARCHAR(10),
                    selskapNavn TEXT,
                    selskapUUID VARCHAR(50),
                    selskapOrgNr BIGINT,
                    company_org_nr_schema VARCHAR(10),
                    selskapRegistrertTid TIMESTAMP,
                    selskapOppdatertTid TIMESTAMP,
                    selskapRolleUUID VARCHAR(50),
                    selskapRolle TEXT,
                    selskapRolleRegistrertTid TIMESTAMP,
                    selskapRolleOppdatertTid TIMESTAMP,
                    selskapRolleRang TEXT,
                    person_company_role_meta_role_elector_id TEXT,
                    person_company_role_meta_role_responsibility TEXT,
                    person_company_role_meta_role_responsibility_percentage REAL,
                    personSelskapRolleUUID VARCHAR(50),
                    personSelskapRolleStartdato DATE,
                    personSelskapRolleSluttdato DATE,
                    personSelskapRollePersonUUID VARCHAR(50),
                    person_company_role_business_key TEXT,
                    personSelskapRolleSelskapUUID VARCHAR(50),
                    person_company_role_external_url TEXT,
                    person_company_role_resigned_flag TEXT,
                    person_company_role_surrogate_key VARCHAR(20),
                    person_company_role_surrugate_key TEXT,
                    person_company_role_data_source_uuid VARCHAR(50),
                    personSelskapRolleRegistrertTid TIMESTAMP,
                    personSelskapRolleOppdatertTid TIMESTAMP,
                    personSelskapRolleSelskapRolleUUID VARCHAR(50)
                );
                """,
                "TRUNCATE person RESTART IDENTITY;",
                "C:/Users/wren9/git/coderintheloop/data/postgres/persons_active_companies_pop_100925_v3_keep.csv",
                "COPY person FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',')",
                "Loading person"
            )
            drop_columns(cur, "person", [
                "nr", "person_birth_day", "person_birth_month", "person_surrogate_key", "person_surrugate_key",
                "person_data_origin_ids", "person_disambiguate_uuid", "person_person_location_type_key",
                "person_national_identification_number", "person_national_identification_schema",
                "person_company_role_meta_role_elector_id", "person_company_role_meta_role_responsibility",
                "person_company_role_meta_role_responsibility_percentage", "person_company_role_external_url",
                "person_company_role_resigned_flag", "person_company_role_surrogate_key",
                "person_company_role_surrugate_key", "person_company_role_data_source_uuid",
                "person_street_name", "person_street_letter", "person_street_number",
                "person_person_master_uuid", "person_composite_business_key", "company_org_nr_schema",
                "person_company_role_business_key", "personSelskapRollePersonUUID",
                "personSelskapRolleSelskapUUID", "personSelskapRolleSelskapRolleUUID"
            ], "person")

            # --- TABLE eierskap ---
            create_and_load_table(
                cur,
                """
                CREATE TABLE IF NOT EXISTS eierskap (
                    nr INTEGER,
                    eierPersonUUID VARCHAR(50),
                    shareholder_person_birth_day REAL,
                    eierPersonNavn TEXT,
                    eierPersonFødselsdato DATE,
                    eierPersonFødselsår REAL,
                    shareholder_person_birth_month REAL,
                    eierPersonKjønnUUID VARCHAR(50),
                    eierPersonPostkode SMALLINT,
                    eierPersonPoststed TEXT,
                    eierPersonAdresse TEXT,
                    eierPersonKommuneNr SMALLINT,
                    eierPersonKommune TEXT,
                    eierSelskapNavn TEXT,
                    eierSelskapUUID VARCHAR(50),
                    eierSelskapOrgNr INTEGER,
                    utstederNavn TEXT,
                    utstederUUID VARCHAR(50),
                    utstederOrgNr INTEGER,
                    eierskapUUID VARCHAR(50),
                    eierskapÅr SMALLINT,
                    eierskapAndel REAL,
                    eierskapAntall BIGINT,
                    company_share_ownership_ownership_lower REAL,
                    company_share_ownership_ownership_upper REAL,
                    eierskapAksjonær TEXT,
                    eierskapStemmeandel REAL,
                    eierskapTotalAntall BIGINT,
                    eierskapStemmeantall BIGINT,
                    company_share_ownership_voting_ownership_lower REAL,
                    company_share_ownership_voting_ownership_upper REAL,
                    company_share_ownership_shareholder_person_uuid VARCHAR(50),
                    company_share_ownership_shareholder_company_uuid VARCHAR(50),
                    eierskapTotalStemmeantall BIGINT,
                    company_share_ownership_share_issuer_company_uuid VARCHAR(50)
                );
                """,
                "TRUNCATE eierskap RESTART IDENTITY;",
                "C:/Users/wren9/git/coderintheloop/data/postgres/ownerships_2023_2025.csv",
                "COPY eierskap FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',')",
                "Loading eierskap"
            )
            drop_columns(cur, "eierskap", [
                "nr", "shareholder_person_birth_month", "shareholder_person_birth_day",
                "company_share_ownership_ownership_lower", "company_share_ownership_ownership_upper",
                "company_share_ownership_voting_ownership_lower", "company_share_ownership_voting_ownership_upper",
                "company_share_ownership_share_issuer_company_uuid", "company_share_ownership_shareholder_person_uuid",
                "company_share_ownership_shareholder_company_uuid"
            ], "eierskap")

            print("[SUCCESS] All tables loaded and cleaned.")

if __name__ == "__main__":
    main()