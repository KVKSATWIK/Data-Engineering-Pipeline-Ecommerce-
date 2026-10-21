
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, countDistinct

spark = (
    SparkSession.builder
    .appName("RevenueAnalysis")
    .getOrCreate()
)

base = "/home/satwi/ecommerce-data/processed"

orders = spark.read.option("header", True).csv(
    f"{base}/orders"
)

payments = spark.read.option("header", True).csv(
    f"{base}/payments"
)

# Convert monetary values to decimal
orders = orders.withColumn(
    "total_amount",
    col("total_amount").cast("decimal(12,2)")
)

payments = payments.withColumn(
    "amount",
    col("amount").cast("decimal(12,2)")
)

# 1. Payment amounts by status
print("=== Payment Amounts by Status ===")

payments.groupBy("payment_status").agg(
    countDistinct("payment_id").alias("payment_count"),
    spark_sum("amount").alias("total_payment_amount")
).orderBy("payment_status").show(truncate=False)

# 2. Amount from completed payments only
completed = payments.filter(
    col("payment_status") == "Completed"
)

print("=== Completed Payments ===")
print("Completed payment count:", completed.count())

completed.agg(
    spark_sum("amount").alias("completed_payment_amount")
).show()

# 3. Join orders with payments
joined = orders.join(
    payments.select("order_id", "payment_status", "amount"),
    on="order_id",
    how="left"
)

print("=== Order Status vs Payment Status ===")

joined.groupBy(
    "order_status", "payment_status"
).count().orderBy(
    "order_status", "payment_status"
).show(truncate=False)

# 4. Delivered orders with completed payments
delivered_paid = joined.filter(
    (col("order_status") == "Delivered") &
    (col("payment_status") == "Completed")
)

print(
    "Delivered orders with completed payments:",
    delivered_paid.select("order_id").distinct().count()
)

delivered_paid.agg(
    spark_sum("amount").alias("payment_amount_for_delivered_paid_orders")
).show()

spark.stop()
