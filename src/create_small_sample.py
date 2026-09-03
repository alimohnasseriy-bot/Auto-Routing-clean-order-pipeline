import csv
import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import SMALL_SAMPLE_ROWS


def create_sample(input_file, output_file, rows):
    input_path = Path(input_file)
    output_path = Path(output_file)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with input_path.open("r", encoding="utf-8-sig", newline="") as infile:
        reader = csv.reader(infile)

        header = next(reader)

        with output_path.open("w", encoding="utf-8", newline="") as outfile:
            writer = csv.writer(outfile)
            writer.writerow(header)

            count = 0

            for row in reader:
                writer.writerow(row)
                count += 1

                if count >= rows:
                    break

    print("=" * 60)
    print("SMALL SAMPLE CREATED")
    print("=" * 60)
    print(f"Input : {input_path}")
    print(f"Output: {output_path}")
    print(f"Rows  : {count}")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create a reproducible small CSV sample."
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to the original CSV file"
    )

    parser.add_argument(
        "--output",
        default="data/orders_small_sample.csv",
        help="Output CSV path"
    )

    parser.add_argument(
        "--rows",
        type=int,
        default=SMALL_SAMPLE_ROWS,
        help="Number of data rows to copy"
    )

    args = parser.parse_args()

    create_sample(
        args.input,
        args.output,
        args.rows
    )
