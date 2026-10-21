
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

BASE_PATH = os.path.expanduser("~/ecommerce-data/processed")

PRIMARY_KEYS = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
}

RELATIONSHIPS = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("payments", "order_id", "orders", "order_id"),
]


def main():
    spark = (
        SparkSession.builder
        .appName("ValidateAllTables")
        .master("local[*]")
        .getOrCreate()
    )

    errors = []

    try:
        tables = {}

        print("=" * 60)
        print("PROCESSED DATA VALIDATION")
        print("=" * 60)

        # 1. Load processed tables and check required files/columns
        print("\n========== TABLE AND SCHEMA CHECKS ==========")

        for table_name, primary_key in PRIMARY_KEYS.items():
            table_path = f"{BASE_PATH}/{table_name}"

            print(f"\n--- {table_name.upper()} ---")

            try:
                df = (
                    spark.read
                    .option("header", True)
                    .option("inferSchema", True)
                    .csv(table_path)
                )

                if primary_key not in df.columns:
                    errors.append(
                        f"{table_name}: Missing primary-key column "
                        f"'{primary_key}'"
                    )
                    print(f"FAIL: Missing column '{primary_key}'")
                    continue

                tables[table_name] = df

                row_count = df.count()
                print(f"Rows: {row_count}")
                print(f"Columns: {len(df.columns)}")

                if row_count == 0:
                    errors.append(
                        f"{table_name}: Processed table is empty"
                    )
                    print("FAIL: Table is empty")

            except Exception as exc:
                errors.append(
                    f"{table_name}: Could not read processed data ({exc})"
                )
                print(f"FAIL: Could not read table: {exc}")

        # Check all relationship columns before validating keys
        for child, child_key, parent, parent_key in RELATIONSHIPS:
            if child in tables and child_key not in tables[child].columns:
                errors.append(
                    f"{child}: Missing relationship column '{child_key}'"
                )

            if parent in tables and parent_key not in tables[parent].columns:
                errors.append(
                    f"{parent}: Missing relationship column '{parent_key}'"
                )

        # 2. Primary-key validation
        print("\n========== PRIMARY KEY VALIDATION ==========")

        for table_name, primary_key in PRIMARY_KEYS.items():
            if table_name not in tables:
                continue

            df = tables[table_name]

            if primary_key not in df.columns:
                continue

            null_count = df.filter(
                col(primary_key).isNull()
            ).count()

            duplicate_groups = (
                df.groupBy(primary_key)
                .count()
                .filter(
                    col(primary_key).isNotNull()
                    & (col("count") > 1)
                )
                .count()
            )

            print(f"\n--- {table_name.upper()} ---")
            print(f"Null primary keys: {null_count}")
            print(f"Duplicate key groups: {duplicate_groups}")

            if null_count > 0:
                errors.append(
                    f"{table_name}: {null_count} null primary keys"
                )

            if duplicate_groups > 0:
                errors.append(
                    f"{table_name}: {duplicate_groups} duplicate "
                    f"primary-key groups"
                )

        # 3. Foreign-key validation
        print("\n========== FOREIGN KEY VALIDATION ==========")

        for child, child_key, parent, parent_key in RELATIONSHIPS:
            if child not in tables or parent not in tables:
                continue

            child_df = tables[child]
            parent_df = tables[parent]

            if child_key not in child_df.columns:
                continue

            if parent_key not in parent_df.columns:
                continue

            null_fk_count = child_df.filter(
                col(child_key).isNull()
            ).count()

            parent_keys = (
                parent_df
                .filter(col(parent_key).isNotNull())
                .select(col(parent_key).alias("_parent_key"))
                .distinct()
            )

            unmatched = child_df.filter(
                col(child_key).isNotNull()
            ).join(
                parent_keys,
                col(child_key) == col("_parent_key"),
                "left_anti",
            )

            unmatched_count = unmatched.count()

            print(f"\n{child}.{child_key} -> {parent}.{parent_key}")
            print(f"Null foreign keys: {null_fk_count}")
            print(f"Unmatched foreign keys: {unmatched_count}")

            if null_fk_count > 0:
                errors.append(
                    f"{child}.{child_key}: {null_fk_count} null "
                    f"foreign keys"
                )

            if unmatched_count > 0:
                errors.append(
                    f"{child}.{child_key}: {unmatched_count} "
                    f"unmatched foreign keys"
                )
                unmatched.select(child_key).show(10, truncate=False)

        # 4. Final result
        print("\n" + "=" * 60)
        print("VALIDATION SUMMARY")
        print("=" * 60)

        if errors:
            for error in errors:
                print(f"FAIL: {error}")

            print(f"\nVALIDATION FAILED: {len(errors)} issue(s)")
            sys.exit(1)

        print("PROCESSED DATA VALIDATION PASSED")
        print("No critical table, primary-key, or foreign-key errors found.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
