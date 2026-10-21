
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

spark = (
    SparkSession.builder
    .appName("EcommerceBusinessMetrics")
    .getOrCreate()
)

path = "/home/satwi/ecommerce-data/analytics/order_lines"

df = (
    spark.read
    .option("header", True)
    .csv(path)
)

# 1. Total unique orders
total_orders = df.select(
    countDistinct("order_id").alias("total_orders")
)

print("=== Total Unique Orders ===")
total_orders.show()

# 2. Orders by status
orders_by_status = (
    df.select("order_id", "order_status")
    .dropDuplicates(["order_id"])
    .groupBy("order_status")
    .count()
    .orderBy(col("count").desc())
)

print("=== Orders by Status ===")
orders_by_status.show()

spark.stop()
