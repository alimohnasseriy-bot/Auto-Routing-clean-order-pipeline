"""
src/main.py
===========
Primary entry point for the midterm-data-pipeline.

Usage
-----
    # Small file (routes to Python Batch automatically)
    python -m src.main --input data/orders_small_sample.csv

    # Large file (routes to PySpark automatically)
    python -m src.main --input data/orders_huge_mixed_quality.csv

    # Path B — incremental loading demo
    python -m src.main --input data/orders_small_sample.csv --demo-path-b

    # Setup MongoDB only
    python -m src.main --setup-only

Pipeline flow
-------------
1. Parse CLI arguments
2. Setup MongoDB (collections + indexes)
3. Route input file (Python Batch or PySpark)
4. RAW LOAD → orders_raw
5. QUALITY PROCESSING → orders_validated + orders_quarantine
6. Collect metrics → reports/results.json
7. (Optional) PATH B demo
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    BATCH_SIZE,
    INPUT_DIR,
    MONGODB_DATABASE,
    MONGODB_URI,
    SMALL_FILE_THRESHOLD_MB,
)


def setup_mongodb() -> None:
    """Ensure MongoDB collections and indexes exist."""
    from src.mongo_setup import setup_database
    setup_database()


def run_pipeline(args: argparse.Namespace) -> dict:
    """Run the full ELT pipeline and return metrics."""
    from src.elt_pipeline import run_pipeline as _run
    return _run(
        file_path=args.input,
        batch_size=args.batch_size,
        mongodb_uri=args.mongo_uri,
        database=args.database,
    )


def write_metrics(run_metrics: dict, args: argparse.Namespace) -> None:
    """Write results.json."""
    from src.metrics import collect_and_write
    collect_and_write(
        run_metrics=run_metrics,
        mongodb_uri=args.mongo_uri,
        database=args.database,
    )


def run_path_b_demo(args: argparse.Namespace) -> None:
    """Run the Path B incremental loading demonstration."""
    from src.incremental_loader import run_path_b_demo as _demo
    _demo(
        sample_path=args.input,
        mongodb_uri=args.mongo_uri,
        database=args.database,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.main",
        description=(
            "midterm-data-pipeline — Hybrid ELT pipeline.\n"
            "Routes small files to Python Batch, large files to PySpark."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.main --input data/orders_small_sample.csv
  python -m src.main --input data/orders_huge_mixed_quality.csv
  python -m src.main --input data/orders_small_sample.csv --demo-path-b
  python -m src.main --setup-only
""",
    )

    parser.add_argument(
        "--input",
        default=str(INPUT_DIR / "orders_small_sample.csv"),
        help="Path to input CSV file  (default: data/orders_small_sample.csv)",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=f"Records per batch insert  (default: {BATCH_SIZE})",
    )

    parser.add_argument(
        "--mongo-uri",
        default=MONGODB_URI,
        help=f"MongoDB connection URI  (default: {MONGODB_URI})",
    )

    parser.add_argument(
        "--database",
        default=MONGODB_DATABASE,
        help=f"MongoDB database name  (default: {MONGODB_DATABASE})",
    )

    parser.add_argument(
        "--setup-only",
        action="store_true",
        help="Only setup MongoDB collections and indexes, then exit",
    )

    parser.add_argument(
        "--demo-path-b",
        action="store_true",
        help="After pipeline, run the Path B incremental loading demonstration",
    )

    parser.add_argument(
        "--skip-pipeline",
        action="store_true",
        help="Skip main pipeline (useful with --demo-path-b if data already loaded)",
    )

    args = parser.parse_args()

    # -----------------------------------------------------------------------
    # Step 1: MongoDB setup
    # -----------------------------------------------------------------------
    print()
    print("=" * 70)
    print("MIDTERM DATA PIPELINE -- PATH B: Incremental Loading")
    print("=" * 70)

    setup_mongodb()

    if args.setup_only:
        print("\nSetup complete. Exiting.")
        return 0

    # -----------------------------------------------------------------------
    # Step 2: Validate input file
    # -----------------------------------------------------------------------
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"\nERROR: Input file not found: {args.input}")
        print(f"Hint : Run  python src/create_small_sample.py --input <large_csv>  first.")
        return 1

    # -----------------------------------------------------------------------
    # Step 3: Run ELT pipeline (unless --skip-pipeline)
    # -----------------------------------------------------------------------
    run_metrics: dict = {}

    if not args.skip_pipeline:
        run_metrics = run_pipeline(args)

        # -----------------------------------------------------------------------
        # Step 4: Write metrics
        # -----------------------------------------------------------------------
        write_metrics(run_metrics, args)

    # -----------------------------------------------------------------------
    # Step 5: Path B demo (optional)
    # -----------------------------------------------------------------------
    if args.demo_path_b:
        run_path_b_demo(args)

    print("\nPipeline finished successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
