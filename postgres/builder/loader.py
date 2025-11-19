from pathlib import Path
import psycopg
from psycopg import sql


def run_sql_file(cur, filepath: str | Path):
    """
    Execute all SQL commands in a .sql file.
    """
    filepath = Path(filepath)
    with open(filepath, "r", encoding="utf-8") as f:
        cur.execute(f.read())  # type: ignore[arg-type]


def load_table(
    cur,
    name: str,
    schema_path: Path,
    csv_path: Path,
    delimiter: str = ",",
    drop_cols: list[str] | None = None,
    date_style: str | None = None,
):
    """
    Create a table from schema SQL, truncate, and load CSV data using COPY.
    Supports:
        - custom delimiter
        - dropping columns after load
        - setting datestyle (e.g. "SQL, DMY")
    """

    print(f"[INFO] Loading table: {name}")

    # Set date style if needed
    if date_style:
        cur.execute(f"SET datestyle = {date_style};")  # type: ignore[arg-type]

    # 1. Create table (schema SQL)
    schema_path = Path(schema_path)
    with open(schema_path, "r", encoding="utf-8") as f:
        cur.execute(f.read())  # type: ignore[arg-type]

    # 2. Truncate (safe even if table just created)
    cur.execute(sql.SQL("TRUNCATE {} RESTART IDENTITY;").format(sql.Identifier(name)))  # type: ignore[arg-type]

    # 3. Load CSV using COPY
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    copy_sql = f"""
        COPY {name}
        FROM STDIN
        WITH (FORMAT CSV, HEADER, DELIMITER '{delimiter}')
    """

    with open(csv_path, "r", encoding="utf-8") as f, cur.copy(copy_sql) as copy:  # type: ignore[arg-type]
        for line in f:
            copy.write(line)

    # 4. Drop columns if needed
    if drop_cols:
        drops = ", ".join([f"DROP COLUMN IF EXISTS {col}" for col in drop_cols])
        alter_sql = f"ALTER TABLE {name} {drops};"
        cur.execute(alter_sql)  # type: ignore[arg-type]

    print(f"[OK] Table '{name}' loaded successfully.")
