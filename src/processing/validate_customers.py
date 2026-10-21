
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, current_date

# 1. Create a Spark session
spark = (
    SparkSession.builder
    .appName("ValidateCustomers")
    .master("local[*]")
    .getOrCreate()
)

# 2. Read the raw customer data
df = spark.read.csv(
    "data/raw/customers.csv",
    header=True,
    inferSchema=True
)

# 3. Basic data-quality checks
total_rows = df.count()

missing_values = df.select(
    [
        spark_sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]
).first().asDict()

unique_customer_ids = df.select("customer_id").distinct().count()

invalid_ids = df.filter(col("customer_id") <= 0).count()

future_signups = df.filter(
    col("signup_date") > current_date()
).count()

# 4. Print a validation report
print("\n===== CUSTOMER DATA QUALITY REPORT =====")
print("Total rows:", total_rows)
print("Missing values by column:", missing_values)
print("Unique customer IDs:", unique_customer_ids)
print("Duplicate customer IDs:", total_rows - unique_customer_ids)
print("Invalid customer IDs:", invalid_ids)
print("Future signup dates:", future_signups)
print("========================================\n")

# 5. Stop Spark
spark.stop()
