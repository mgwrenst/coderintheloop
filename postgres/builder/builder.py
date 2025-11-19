from pathlib import Path

from .config_loader import load_config
from .connection import connect_to_database
from .loader import load_table, run_sql_file


def build_database(config_path: str | Path):
    """
    Build a Postgres database from a YAML config.
    Steps:
        - load YAML config
        - create database if needed
        - connect to the database
        - load each table (schema + CSV)
        - run optional post-load SQL scripts
    """

    # Load YAML config
    cfg = load_config(config_path)

    dbname = cfg["database"]
    tables = cfg.get("tables", [])
    post_sql_files = cfg.get("post_load_sql", [])

    print(f"\n=== Building tables in database '{dbname}' ===")


    # Connect to the database
    with connect_to_database(dbname) as conn, conn.cursor() as cur:

        # Load each table
        for table_cfg in tables:
            name = table_cfg["name"]

            try:
                cur.execute("SAVEPOINT before_table_load;")  # type: ignore[arg-type]

                load_table(
                    cur=cur,
                    name=name,
                    schema_path=table_cfg["schema"],
                    csv_path=table_cfg["file"],
                    delimiter=table_cfg.get("delimiter", ","),
                    drop_cols=table_cfg.get("drop_cols"),
                    date_style=table_cfg.get("date_style"),
                )

            except Exception as e:
                print(f"[ERROR] Failed loading table '{name}': {e}")
                cur.execute("ROLLBACK TO SAVEPOINT before_table_load;")  # type: ignore[arg-type]

        # Run optional cleanup / index SQL files
        for sql_file in post_sql_files:
            sql_path = Path(sql_file)
            if not sql_path.exists():
                print(f"[WARNING] Post-load SQL file not found: {sql_path}")
                continue

            print(f"[INFO] Running post-load SQL: {sql_path}")
            run_sql_file(cur, sql_path)

        conn.commit()

    print(f"[SUCCESS] Database '{dbname}' built successfully.\n")
