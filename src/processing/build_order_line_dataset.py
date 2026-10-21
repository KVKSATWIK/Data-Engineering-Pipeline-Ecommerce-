
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("BuildOrderLineDataset")
    .getOrCreate()
)

base_path = "/home/satwi/ecommerce-data/processed"


# Read the cleaned datasets
customers = spark.read.option("header", True).csv(
    f"{base_path}/customers"
)

orders = spark.read.option("header", True).csv(
    f"{base_path}/orders"
)

order_items = spark.read.option("header", True).csv(
    f"{base_path}/order_items"
)

products = spark.read.option("header", True).csv(
    f"{base_path}/products"
)

# Rename overlapping columns before joining
orders = orders.withColumnRenamed(
    "total_amount", "order_total"
)

order_items = order_items.withColumnRenamed(
    "unit_price", "item_unit_price"
)

products = products.withColumnRenamed(
    "unit_price", "product_unit_price"
)


# Join orders with customers
orders_customers = orders.join(
    customers,
    on="customer_id",
    how="inner"
)

# Join each order with its individual items
order_lines = orders_customers.join(
    order_items,
    on="order_id",
    how="inner"
)

# Add product details
order_lines = order_lines.join(
    products,
    on="product_id",
    how="inner"
)

# Select and rename the required columns

analytics_df = order_lines.select(
    col("order_item_id"),
    col("order_id"),
    col("customer_id"),
    col("first_name"),
    col("last_name"),
    col("city"),
    col("order_date"),
    col("order_status"),
    col("order_total"),
    col("product_id"),
    col("product_name"),
    col("category"),
    col("quantity"),
    col("item_unit_price"),
    col("product_unit_price")
)


print("Analytics dataset schema:")
analytics_df.printSchema()

print("Total rows:", analytics_df.count())

analytics_df.show(10, truncate=False)

# Save to the Linux filesystem
output_path = "/home/satwi/ecommerce-data/analytics/order_lines"

analytics_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(output_path)

print("Analytics dataset saved to:", output_path)

spark.stop()
