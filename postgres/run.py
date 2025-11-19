import sys
from pathlib import Path

from builder.builder import build_database


def main():
    """
    Run the database builder using:
        python run.py groundtruthsmall
    """
    if len(sys.argv) < 2:
        print("Usage: python run.py <version_name>")
        sys.exit(1)

    version_name = sys.argv[1]

    # Path to the YAML configuration file
    # Example: versions/groundtruthsmall.yaml
    versions_dir = Path(__file__).parent / "versions"
    config_path = versions_dir / f"{version_name}.yaml"

    if not config_path.exists():
        print(f"[ERROR] Version config not found: {config_path}")
        print("Available versions:")
        for f in versions_dir.glob("*.yaml"):
            print(" -", f.stem)
        sys.exit(1)

    print(f"[INFO] Using config: {config_path}")
    build_database(config_path)


if __name__ == "__main__":
    main()
