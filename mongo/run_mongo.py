import sys
from main import build


def main():
    if len(sys.argv) < 3:
        print("Usage: python run_mongo.py <version> <mode: copy|structured|all>")
        sys.exit(1)

    version = sys.argv[1]
    mode = sys.argv[2]

    print(f"\n=== MongoDB build: version='{version}', mode='{mode}' ===")
    ok = build(version, mode)
    if not ok:
        sys.exit(1)
    print(f"=== Completed Mongo build: {version} ({mode}) ===\n")


if __name__ == "__main__":
    main()
