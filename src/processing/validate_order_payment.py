
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

BASE_PATH = Path.home() / "ecommerce-data" / "processed"
OUTPUT_PATH = (
    Path.home() / "ecommerce-data" / "analytics"
    / "order_payment_issues"
)

REQUIRED_COLUMNS = {
    "orders": ["order_id", "order_status"],
    "payments": ["order_id", "payment_status", "amount"],
}

SUSPICIOUS_COMBINATIONS = [
    ("Cancelled", "Completed"),
    ("Delivered", "Failed"),
    ("Delivered", "Refunded"),
]


def main():
    spark = (
        SparkSession.builder
        .appName("ValidateOrderPayment")
        .getOrCreate()
    )

    try:
        print("=" * 60)
        print("ORDER/PAYMENT CONSISTENCY CHECK")
        print("=" * 60)

        # 1. Check that both processed datasets can be read
        try:
            orders = (
                spark.read.option("header", True)
                .csv(str(BASE_PATH / "orders"))
            )

            payments = (
                spark.read.option("header", True)
                .csv(str(BASE_PATH / "payments"))
            )
        except Exception as exc:
            print(f"ERROR: Could not read processed data: {exc}")
            sys.exit(1)

        # 2. Check required columns
        for table_name, df in [
            ("orders", orders),
            ("payments", payments),
        ]:
            missing = [
                name for name in REQUIRED_COLUMNS[table_name]
                if name not in df.columns
            ]

            if missing:
                print(
                    f"ERROR: {table_name} is missing columns: {missing}"
                )
                sys.exit(1)

        # 3. Check order IDs are present
        if orders.filter(col("order_id").isNull()).limit(1).count():
            print("ERROR: Orders contain null order_id values.")
            sys.exit(1)

        if payments.filter(col("order_id").isNull()).limit(1).count():
            print("ERROR: Payments contain null order_id values.")
            sys.exit(1)

        # 4. Check that each order has at most one payment row.
        # This reflects the current project's data model.
        duplicate_payments = (
            payments.groupBy("order_id")
            .count()
            .filter(col("count") > 1)
            .count()
        )

        if duplicate_payments > 0:
            print(
                "ERROR: Multiple payment rows exist for "
                f"{duplicate_payments} order(s)."
            )
            sys.exit(1)

        # 5. Join orders with their payment status
        joined = orders.select(
            "order_id", "order_status"
        ).join(
            payments.select(
                "order_id", "payment_status", "amount"
            ),
            on="order_id",
            how="left",
        )

        # 6. Identify combinations that need human review
        suspicious = joined.filter(
            (
                (col("order_status") == "Cancelled")
                & (col("payment_status") == "Completed")
            )
            |
            (
                (col("order_status") == "Delivered")
                & (col("payment_status") == "Failed")
            )
            |
            (
                (col("order_status") == "Delivered")
                & (col("payment_status") == "Refunded")
            )
        )

        print("\n=== Combinations to Review ===")

        suspicious.groupBy(
            "order_status", "payment_status"
        ).agg(
            count("*").alias("order_count")
        ).orderBy(
            "order_status", "payment_status"
        ).show(truncate=False)

        # 7. Save the review dataset
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

        suspicious.write.mode("overwrite").option(
            "header", True
        ).csv(str(OUTPUT_PATH))

        flagged_count = suspicious.count()

        print(f"Rows flagged for review: {flagged_count}")
        print(f"Review dataset saved to: {OUTPUT_PATH}")
        print(
            "CONSISTENCY CHECK COMPLETED. "
            "Flagged combinations are warnings for investigation."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
