
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_date

spark = (
    SparkSession.builder
    .appName("TransformOrders")
    .master("local[*]")
    .getOrCreate()
)

# 1. Read raw orders
df = spark.read.csv(
    "data/raw/orders.csv",
    header=True,
    inferSchema=True
)

print("Raw orders:")
df.show(5, truncate=False)

# 2. Clean and standardize columns
cleaned_df = (
    df
    .withColumn("order_id", col("order_id").cast("integer"))
    .withColumn("customer_id", col("customer_id").cast("integer"))
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    .withColumn("order_status", trim(col("order_status")))
    .withColumn("total_amount", col("total_amount").cast("decimal(12,2)"))
)

# 3. Check for null values
from pyspark.sql.functions import count, when

print("Missing values by column:")
cleaned_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in cleaned_df.columns
]).show()

# 4. Check order status distribution
print("Order status distribution:")
cleaned_df.groupBy("order_status").count().show()

# 5. Save processed data to Linux filesystem
output_path = os.path.expanduser(
    "~/ecommerce-data/processed/orders"
)

cleaned_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(output_path)

# 6. Verify the result
print("Transformed orders:")
cleaned_df.show(10, truncate=False)

print("Total order rows:", cleaned_df.count())
print("Processed data saved to:", output_path)

spark.stop()
