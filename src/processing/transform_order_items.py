
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, count, when

spark = (
    SparkSession.builder
    .appName("TransformOrderItems")
    .master("local[*]")
    .getOrCreate()
)

# 1. Read raw order items
df = spark.read.csv(
    "data/raw/order_items.csv",
    header=True,
    inferSchema=True
)

print("Raw order items:")
df.show(5, truncate=False)

# 2. Clean and standardize data types
cleaned_df = (
    df
    .withColumn("order_item_id", col("order_item_id").cast("integer"))
    .withColumn("order_id", col("order_id").cast("integer"))
    .withColumn("product_id", col("product_id").cast("integer"))
    .withColumn("quantity", col("quantity").cast("integer"))
    .withColumn("unit_price", col("unit_price").cast("decimal(12,2)"))
)

# 3. Check for missing values
print("Missing values by column:")
cleaned_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in cleaned_df.columns
]).show()

# 4. Check quantity and price
print("Invalid quantity or price:")
cleaned_df.filter(
    (col("quantity") <= 0) |
    (col("unit_price") < 0)
).show(truncate=False)

# 5. Save output to Linux filesystem
output_path = os.path.expanduser(
    "~/ecommerce-data/processed/order_items"
)

cleaned_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(output_path)

# 6. Verify results
print("Transformed order items:")
cleaned_df.show(10, truncate=False)

print("Total order item rows:", cleaned_df.count())
print("Processed data saved to:", output_path)

spark.stop()
