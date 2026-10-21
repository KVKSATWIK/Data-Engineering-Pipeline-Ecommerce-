
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

PATH = "/home/satwi/ecommerce-data/analytics/order_lines"

REQUIRED_COLUMNS = [
    "order_item_id",
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "order_total",
    "item_unit_price",
    "product_unit_price",
]

NUMERIC_COLUMNS = REQUIRED_COLUMNS

def main():
    spark = (
        SparkSession.builder
        .appName("ValidateAnalyticsDataset")
        .getOrCreate()
    )

    errors = []
    warnings = []

    try:
        print("=" * 60)
        print("ANALYTICS DATASET VALIDATION")
        print("=" * 60)

        # 1. Check that the dataset can be read
        try:
            df = spark.read.option("header", True).csv(PATH)
        except Exception as exc:
            print(f"ERROR: Could not read analytics dataset: {exc}")
            sys.exit(1)

        missing_columns = [
            name for name in REQUIRED_COLUMNS
            if name not in df.columns
        ]

        if missing_columns:
            errors.append(
                f"Missing required columns: {missing_columns}"
            )

        if missing_columns:
            for error in errors:
                print(f"ERROR: {error}")
            sys.exit(1)

        total_rows = df.count()

        print(f"Total rows: {total_rows}")
        print(f"Total columns: {len(df.columns)}")

        if total_rows == 0:
            errors.append("Analytics dataset contains no rows")

        # 2. Check missing values
        print("\n--- Missing Values ---")

        missing_counts = df.select([
            count(
                when(
                    col(c).isNull() | (col(c) == ""),
                    c
                )
            ).alias(c)
            for c in df.columns
        ]).first().asDict()

        for column_name, missing_count in missing_counts.items():
            print(f"{column_name}: {missing_count}")

            if missing_count > 0:
                message = (
                    f"{column_name}: {missing_count} missing values"
                )

                # These fields are required to identify and analyse
                # each order line reliably.
                if column_name in REQUIRED_COLUMNS:
                    errors.append(message)
                else:
                    warnings.append(message)

        # 3. Check order-item IDs
        print("\n--- Order Item ID Validation ---")

        null_ids = df.filter(
            col("order_item_id").isNull()
            | (col("order_item_id") == "")
        ).count()

        duplicate_ids = (
            df.groupBy("order_item_id")
            .count()
            .filter(
                col("order_item_id").isNotNull()
                & (col("order_item_id") != "")
                & (col("count") > 1)
            )
            .count()
        )

        print(f"Missing order_item_id values: {null_ids}")
        print(f"Duplicate order_item_id groups: {duplicate_ids}")

        if duplicate_ids > 0:
            errors.append(
                f"{duplicate_ids} duplicate order_item_id groups"
            )

        # 4. Check numeric values
        print("\n--- Numeric Conversion Checks ---")

        for column_name in NUMERIC_COLUMNS:
            invalid_count = df.filter(
                col(column_name).isNotNull()
                & (col(column_name) != "")
                & col(column_name).cast("double").isNull()
            ).count()

            print(
                f"{column_name}: "
                f"invalid numeric values = {invalid_count}"
            )

            if invalid_count > 0:
                errors.append(
                    f"{column_name}: {invalid_count} invalid numeric values"
                )

        # 5. Final result
        print("\n" + "=" * 60)
        print("VALIDATION SUMMARY")
        print("=" * 60)

        for warning in warnings:
            print(f"WARNING: {warning}")

        if errors:
            for error in errors:
                print(f"ERROR: {error}")

            print(f"\nVALIDATION FAILED: {len(errors)} issue(s)")
            sys.exit(1)

        print("ANALYTICS DATASET VALIDATION PASSED")
        print(f"Warnings: {len(warnings)}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
