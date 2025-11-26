import sys
from main import build_database


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run.py <version_name>")
        sys.exit(1)
    
    version_name = sys.argv[1]
    success = build_database(version_name)
    
    if not success:
        sys.exit(1)
