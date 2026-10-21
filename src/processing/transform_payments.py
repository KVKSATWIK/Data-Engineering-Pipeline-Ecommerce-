
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_date, count, when

spark = (
    SparkSession.builder
    .appName("TransformPayments")
    .master("local[*]")
    .getOrCreate()
)

# 1. Read raw payments
df = spark.read.csv(
    "data/raw/payments.csv",
    header=True,
    inferSchema=True
)

print("Raw payments:")
df.show(5, truncate=False)

# 2. Clean and standardize data types
cleaned_df = (
    df
    .withColumn("payment_id", col("payment_id").cast("integer"))
    .withColumn("order_id", col("order_id").cast("integer"))
    .withColumn("payment_date", to_date(col("payment_date"), "yyyy-MM-dd"))
    .withColumn("payment_method", trim(col("payment_method")))
    .withColumn("payment_status", trim(col("payment_status")))
    .withColumn("amount", col("amount").cast("decimal(12,2)"))
)

# 3. Check for missing values
print("Missing values by column:")
cleaned_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in cleaned_df.columns
]).show()

# 4. Check for invalid payment amounts
print("Invalid payment amounts:")
cleaned_df.filter(
    col("amount") < 0
).show(truncate=False)

# 5. Check payment status distribution
print("Payment status distribution:")
cleaned_df.groupBy("payment_status").count().show()

# 6. Save processed data to Linux filesystem
output_path = os.path.expanduser(
    "~/ecommerce-data/processed/payments"
)

cleaned_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(output_path)

# 7. Verify the output
print("Transformed payments:")
cleaned_df.show(10, truncate=False)

print("Total payment rows:", cleaned_df.count())
print("Processed data saved to:", output_path)

spark.stop()
