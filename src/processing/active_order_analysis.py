
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, countDistinct

spark = (
    SparkSession.builder
    .appName("ActiveOrderAnalysis")
    .getOrCreate()
)

path = "/home/satwi/ecommerce-data/analytics/order_lines"

df = spark.read.option("header", True).csv(path)

# Convert numeric columns
df = (
    df.withColumn("quantity", col("quantity").cast("int"))
      .withColumn("item_unit_price", col("item_unit_price").cast("decimal(12,2)"))
)

# Keep one row per order for order-level metrics
orders_df = df.select(
    "order_id", "order_status", "order_total"
).dropDuplicates(["order_id"])

print("=== All Orders by Status ===")
orders_df.groupBy("order_status").count().orderBy(
    col("count").desc()
).show()

# Exclude cancelled orders
active_orders = orders_df.filter(
    col("order_status") != "Cancelled"
)

print("=== Order Summary ===")
print("Total orders:", orders_df.count())
print("Non-cancelled orders:", active_orders.count())
print(
    "Cancelled orders:",
    orders_df.filter(col("order_status") == "Cancelled").count()
)

# Analyze quantities from non-cancelled orders
active_order_ids = active_orders.select("order_id")

active_items = df.join(
    active_order_ids,
    on="order_id",
    how="inner"
)

print("=== Product Quantities in Non-Cancelled Orders ===")
active_items.groupBy("product_id", "product_name").agg(
    spark_sum("quantity").alias("total_quantity_ordered")
).orderBy(
    col("total_quantity_ordered").desc()
).show(10, truncate=False)

spark.stop()
