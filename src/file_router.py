import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import SMALL_FILE_THRESHOLD_MB


def get_file_size_mb(file_path):
    size_bytes = Path(file_path).stat().st_size
    return size_bytes / (1024 * 1024)


def choose_engine(file_path):
    size_mb = get_file_size_mb(file_path)

    if size_mb <= SMALL_FILE_THRESHOLD_MB:
        engine = "python_batch"
        reason = f"file size ({size_mb:.2f} MB) <= {SMALL_FILE_THRESHOLD_MB} MB"
    else:
        engine = "pyspark"
        reason = f"file size ({size_mb:.2f} MB) > {SMALL_FILE_THRESHOLD_MB} MB"

    return size_mb, engine, reason


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python src\\file_router.py <csv_file>")
        sys.exit(1)

    file_path = sys.argv[1]

    if not Path(file_path).exists():
        print(f"ERROR: File not found: {file_path}")
        sys.exit(1)

    size_mb, engine, reason = choose_engine(file_path)

    print("=" * 60)
    print("FILE ROUTER")
    print("=" * 60)
    print(f"File   : {file_path}")
    print(f"Size   : {size_mb:.2f} MB")
    print(f"Limit  : {SMALL_FILE_THRESHOLD_MB} MB")
    print(f"Engine : {engine}")
    print(f"Reason : {reason}")
    print("=" * 60)
