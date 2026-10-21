
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, sum as spark_sum

spark = (
    SparkSession.builder
    .appName("PaymentAnalysis")
    .getOrCreate()
)

base = "/home/satwi/ecommerce-data/processed"

orders = spark.read.option("header", True).csv(
    f"{base}/orders"
)

payments = spark.read.option("header", True).csv(
    f"{base}/payments"
)

# 1. Delivered orders
delivered = orders.filter(col("order_status") == "Delivered")

print("=== Delivered Orders ===")
print("Delivered order count:", delivered.select("order_id").distinct().count())

# 2. Payment status distribution
print("=== Payment Status Distribution ===")
payments.groupBy("payment_status").agg(
    countDistinct("payment_id").alias("payment_count"),
    spark_sum(col("amount").cast("decimal(12,2)")).alias("total_amount")
).orderBy(
    col("payment_status")
).show(truncate=False)

# 3. Check how many payments exist per order
print("=== Payment Records Per Order ===")
payments.groupBy("order_id").agg(
    countDistinct("payment_id").alias("payment_records")
).groupBy("payment_records").count().orderBy(
    "payment_records"
).show()

spark.stop()
