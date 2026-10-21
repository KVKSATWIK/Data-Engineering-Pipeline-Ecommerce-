
import sys
from pathlib import Path

import pandas as pd

RAW_DIR = Path("data/raw")

# Each source table's primary key
PRIMARY_KEYS = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
}

def main():
    errors = []
    warnings = []

    print("=" * 60)
    print("RAW DATA VALIDATION")
    print("=" * 60)

    for table, primary_key in PRIMARY_KEYS.items():
        file_path = RAW_DIR / f"{table}.csv"

        print(f"\n--- {table} ---")

        # 1. Check whether the file exists
        if not file_path.is_file():
            errors.append(f"{table}: CSV file is missing")
            print("ERROR: File not found")
            continue

        # 2. Check whether the CSV can be read
        try:
            df = pd.read_csv(file_path)
        except (OSError, pd.errors.ParserError, UnicodeError) as exc:
            errors.append(f"{table}: Cannot read CSV ({exc})")
            print(f"ERROR: Cannot read CSV: {exc}")
            continue

        # 3. Check whether the file has data and columns
        print(f"Rows: {len(df)}")
        print(f"Columns: {len(df.columns)}")

        if len(df.columns) == 0:
            errors.append(f"{table}: CSV has no columns")
            print("ERROR: No columns found")
            continue

        if df.empty:
            errors.append(f"{table}: CSV contains no data rows")
            print("ERROR: No data rows found")
            continue

        # 4. Check for the expected primary-key column
        if primary_key not in df.columns:
            errors.append(
                f"{table}: Required primary-key column "
                f"'{primary_key}' is missing"
            )
            print(f"ERROR: Missing column '{primary_key}'")
            continue

        # 5. Check primary keys for missing and duplicate values
        missing_keys = int(df[primary_key].isna().sum())
        duplicate_keys = int(
            df[primary_key].dropna().duplicated().sum()
        )

        print(f"Missing {primary_key}: {missing_keys}")
        print(f"Duplicate {primary_key}: {duplicate_keys}")

        if missing_keys:
            errors.append(
                f"{table}: {missing_keys} missing primary keys"
            )

        if duplicate_keys:
            errors.append(
                f"{table}: {duplicate_keys} duplicate primary keys"
            )

        # 6. Report missing values in other columns
        missing_by_column = df.drop(
            columns=[primary_key]
        ).isna().sum()

        missing_by_column = missing_by_column[
            missing_by_column > 0
        ]

        total_missing = int(df.isna().sum().sum())
        print(f"Total missing values: {total_missing}")

        if not missing_by_column.empty:
            details = ", ".join(
                f"{column}={int(count)}"
                for column, count in missing_by_column.items()
            )
            warning = f"{table}: Missing values: {details}"
            warnings.append(warning)
            print(f"WARNING: {details}")

    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)

    for warning in warnings:
        print(f"WARNING: {warning}")

    for error in errors:
        print(f"ERROR: {error}")

    if errors:
        print(f"\nVALIDATION FAILED: {len(errors)} error(s)")
        sys.exit(1)

    print(f"\nRAW DATA VALIDATION PASSED")
    print(f"Warnings: {len(warnings)}")
    print("No critical validation errors found.")


if __name__ == "__main__":
    main()
