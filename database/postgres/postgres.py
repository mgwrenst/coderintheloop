import os
from dotenv import load_dotenv
import psycopg

# --- Load env ---
load_dotenv()

def get_conninfo():
    """Return Postgres connection parameters from environment."""
    env_map = {
        "host": "PGHOST",
        "port": "PGPORT",
        "user": "PGUSER",
        "password": "PGPASSWORD",
        "dbname": "PGDATABASE",
    }
    return {k: os.getenv(v) for k, v in env_map.items() if os.getenv(v)}

def load_table(cur, name, create_sql, file, delimiter, drop_cols=None, date_style=None):
    """Create, truncate, and load a table from CSV, optionally drop columns."""
    print(f"[INFO] Loading {name} ...")
    if date_style:
        cur.execute(f"SET datestyle = {date_style};")

    cur.execute(create_sql)
    cur.execute(f"TRUNCATE {name} RESTART IDENTITY;")

    with open(file, encoding="utf-8") as f, cur.copy(
        f"COPY {name} FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER '{delimiter}')"
    ) as copy:
        for line in f:
            copy.write(line)

    if drop_cols:
        drops = ", ".join([f"DROP COLUMN IF EXISTS {c}" for c in drop_cols])
        cur.execute(f"ALTER TABLE {name} {drops};")

    print(f"[OK] {name} loaded.")

def main():
    conninfo = get_conninfo()
    with psycopg.connect(**conninfo) as conn, conn.cursor() as cur:

        # --- aksjeeiebok ---
        load_table(cur, "aksjeeiebok", """
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
        """, "data/postgres/aksjeeiebok.csv", ";")

        # --- politikere ---
        load_table(cur, "politikere", """
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
        """, "data/postgres/politikere.csv", ";", date_style="DMY")

        # --- konkurs ---
        load_table(cur, "konkurs", """
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
        """, "data/postgres/bankrupt_2020_120925.csv", ",", drop_cols=["nr"])

        # --- selskap ---
        load_table(cur, "selskap", """
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
        """, "data/postgres/companies_active_companies_pop_100925_v3.csv", ",", drop_cols=["nr"])

        # --- remove overlap & create view ---
        cur.execute("DELETE FROM selskap WHERE UUID IN (SELECT UUID FROM konkurs);")
        cur.execute("""
            CREATE OR REPLACE VIEW alleSelskaper AS
            SELECT * FROM selskap
            UNION
            SELECT * FROM konkurs;
        """)
        print("[OK] View alleSelskaper created.")

        # --- person ---
        load_table(cur, "person", """
            CREATE TABLE IF NOT EXISTS person (
                nr INTEGER,
                UUID VARCHAR(50),
                navn TEXT,
                fødselsdato DATE,
                fødselsår REAL,
                land TEXT,
                postkode TEXT,
                adresse TEXT,
                landKode VARCHAR(10),
                registrertTid TIMESTAMP,
                oppdatertTid TIMESTAMP,
                kommuneNr SMALLINT,
                kommuneNavn TEXT,
                selskapNavn TEXT,
                selskapUUID VARCHAR(50),
                selskapOrgNr BIGINT,
                selskapRolleUUID VARCHAR(50),
                selskapRolle TEXT,
                selskapRolleRegistrertTid TIMESTAMP,
                selskapRolleOppdatertTid TIMESTAMP,
                selskapRolleRang TEXT
            );
        """, "data/postgres/persons_active_companies_pop_100925_v3_keep.csv", ",",
        drop_cols=["nr"])

        # --- eierskap ---
        load_table(cur, "eierskap", """
            CREATE TABLE IF NOT EXISTS eierskap (
                nr INTEGER,
                eierPersonUUID VARCHAR(50),
                eierPersonNavn TEXT,
                eierPersonFødselsdato DATE,
                eierSelskapNavn TEXT,
                eierSelskapUUID VARCHAR(50),
                eierSelskapOrgNr INTEGER,
                utstederNavn TEXT,
                utstederUUID VARCHAR(50),
                utstederOrgNr INTEGER,
                eierskapUUID VARCHAR(50),
                eierskapÅr SMALLINT,
                eierskapAndel REAL,
                eierskapAntall BIGINT
            );
        """, "data/postgres/ownerships_2023_2025.csv", ",", drop_cols=["nr"])

        print("[SUCCESS] All tables loaded and cleaned.")

if __name__ == "__main__":
    main()
