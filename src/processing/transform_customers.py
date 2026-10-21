
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower

# 1. Create a Spark session
spark = (
    SparkSession.builder
    .appName("TransformCustomers")
    .master("local[*]")
    .config("spark.hadoop.hadoop.security.authentication", "simple")
    .getOrCreate()
)

print(
    "Hadoop version:",
    spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
)


# 2. Read the raw customer data
df = spark.read.csv(
    "data/raw/customers.csv",
    header=True,
    inferSchema=True
)

# 3. Clean text columns
cleaned_df = (
    df
    .withColumn("first_name", trim(col("first_name")))
    .withColumn("last_name", trim(col("last_name")))
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("city", trim(col("city")))
)

output_path = os.path.expanduser(
    "~/ecommerce-data/processed/customers"
)

cleaned_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(output_path)

# 6. Display a preview
print("Transformed customer data:")
cleaned_df.show(10, truncate=False)

print("Rows in processed DataFrame:", cleaned_df.count())
print("Processed data saved to:", output_path)

# 7. Stop Spark
spark.stop()
