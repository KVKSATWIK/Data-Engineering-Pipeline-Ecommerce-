
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

spark = (
    SparkSession.builder
    .appName("TransformProducts")
    .master("local[*]")
    .getOrCreate()
)

# 1. Read raw product data
df = spark.read.csv(
    "data/raw/products.csv",
    header=True,
    inferSchema=True
)

print("Raw product data:")
df.show(5, truncate=False)

# 2. Clean and standardize the data
cleaned_df = (
    df
    .withColumn("product_id", col("product_id").cast("integer"))
    .withColumn("product_name", trim(col("product_name")))
    .withColumn("category", trim(col("category")))
    .withColumn("unit_price", col("unit_price").cast("decimal(10,2)"))
    .withColumn("stock_quantity", col("stock_quantity").cast("integer"))
)

# 3. Check for missing values
print("Missing values by column:")
cleaned_df.select([
    __import__("pyspark").sql.functions.count(
        __import__("pyspark").sql.functions.when(
            col(c).isNull(), c
        )
    ).alias(c)
    for c in cleaned_df.columns
]).show()

# 4. Save output to the Linux filesystem
output_path = os.path.expanduser(
    "~/ecommerce-data/processed/products"
)

cleaned_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(output_path)

# 5. Verify output
print("Transformed product data:")
cleaned_df.show(10, truncate=False)

print("Total product rows:", cleaned_df.count())
print("Processed data saved to:", output_path)

spark.stop()
