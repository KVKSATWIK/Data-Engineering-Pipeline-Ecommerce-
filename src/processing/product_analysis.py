
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

spark = (
    SparkSession.builder
    .appName("ProductAnalysis")
    .getOrCreate()
)

path = "/home/satwi/ecommerce-data/analytics/order_lines"

df = spark.read.option("header", True).csv(path)

# Convert quantity from string to integer
df = df.withColumn(
    "quantity",
    col("quantity").cast("integer")
)

# 1. Top 10 products by quantity ordered
top_products = (
    df.groupBy("product_id", "product_name")
    .agg(spark_sum("quantity").alias("total_quantity"))
    .orderBy(col("total_quantity").desc())
)

print("=== Top 10 Products by Quantity Ordered ===")
top_products.show(10, truncate=False)

# 2. Product category analysis
category_analysis = (
    df.groupBy("category")
    .agg(spark_sum("quantity").alias("total_quantity"))
    .orderBy(col("total_quantity").desc())
)

print("=== Quantity Ordered by Category ===")
category_analysis.show(truncate=False)

spark.stop()
