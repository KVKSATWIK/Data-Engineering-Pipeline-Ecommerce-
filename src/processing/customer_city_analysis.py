
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

spark = (
    SparkSession.builder
    .appName("CustomerCityAnalysis")
    .getOrCreate()
)

path = "/home/satwi/ecommerce-data/analytics/order_lines"

df = spark.read.option("header", True).csv(path)

# Keep one row per order to avoid counting each item as an order
orders_df = df.select(
    "order_id",
    "customer_id",
    "first_name",
    "last_name",
    "city",
    "order_status"
).dropDuplicates(["order_id"])

# 1. Number of orders by city
city_analysis = (
    orders_df.groupBy("city")
    .agg(countDistinct("order_id").alias("total_orders"))
    .orderBy(col("total_orders").desc())
)

print("=== Orders by City ===")
city_analysis.show(truncate=False)

# 2. Top 10 customers by number of orders
customer_analysis = (
    orders_df.groupBy(
        "customer_id", "first_name", "last_name"
    )
    .agg(countDistinct("order_id").alias("total_orders"))
    .orderBy(col("total_orders").desc())
)

print("=== Top 10 Customers by Orders ===")
customer_analysis.show(10, truncate=False)

spark.stop()
