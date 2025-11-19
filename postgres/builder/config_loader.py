import yaml
from pathlib import Path

def load_config(config_path: str | Path) -> dict:
    """
    Load a YAML config file and normalize important paths.
    Returns a dict with:
        - database
        - csv_base_path
        - schema_base_path
        - tables
        - post_load_sql
    """

    config_path = Path(config_path)

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Covert base paths to path objects
    if "csv_base_path" in config:
        config["csv_base_path"] = Path(config["csv_base_path"])
    
    if "schema_base_path" in config:
        config["schema_base_path"] = Path(config["schema_base_path"])

    # Make table schema and file paths relative to base dirs
    for table in config.get("tables", []):
        if "schema" in table and config.get("schema_base_path"):
            table["schema"] = config["schema_base_path"] / table["schema"]
        if "file" in table and config.get("csv_base_path"):
            table["file"] = config["csv_base_path"] / table["file"]
    
    return config