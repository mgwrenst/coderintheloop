import sys
from pathlib import Path
import yaml

from builder.copy_builder import build_copy
from builder.structured_builder import build_structured
from builder.pg_utils import get_pg_conninfo


def main():
    if len(sys.argv) < 3:
        print("Usage: python run_mongo.py <version> <mode: copy|structured>")
        sys.exit(1)

    version = sys.argv[1]
    mode = sys.argv[2]

    # Load YAML config
    config_path = Path(__file__).parent / "versions" / f"{version}.yaml"
    if not config_path.exists():
        print(f"[ERROR] Config not found: {config_path}")
        sys.exit(1)

    cfg = yaml.safe_load(config_path.read_text())

    source_pg_db = cfg["source_postgres"]
    target_mongo_copy = cfg["target_mongo_copy"]
    target_mongo_structured = cfg["target_mongo_structured"]
    tables = cfg.get("tables", [])

    # Build connection info for Postgres
    pg_conninfo = get_pg_conninfo(source_pg_db)

    print(f"\n=== MongoDB build: version='{version}', mode='{mode}' ===")

    if mode == "copy":
        print(f"→ Copying Postgres '{source_pg_db}' → Mongo '{target_mongo_copy}'")
        build_copy(pg_conninfo, target_mongo_copy, tables)

    elif mode == "structured":
        print(f"→ Structuring Mongo '{target_mongo_structured}' from raw '{target_mongo_copy}'")
        build_structured(target_mongo_copy, target_mongo_structured)

    else:
        print(f"[ERROR] Unknown mode '{mode}'. Use 'copy' or 'structured'.")
        sys.exit(1)

    print(f"=== Completed Mongo build: {version} ({mode}) ===\n")


if __name__ == "__main__":
    main()
