
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# 1. Create a Spark session
spark = (
    SparkSession.builder
    .appName("ReadCustomers")
    .master("local[*]")
    .getOrCreate()
)

# 2. Read the raw customers CSV file
df = spark.read.csv(
    "data/raw/customers.csv",
    header=True,
    inferSchema=True
)

# 3. Check missing values in every column
df.select(
    [
        sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]
).show()


# Check for duplicate customer IDs
total_rows = df.count()
unique_customer_ids = df.select("customer_id").distinct().count()

print("Total rows:", total_rows)
print("Unique customer IDs:", unique_customer_ids)
print("Duplicate customer IDs:", total_rows - unique_customer_ids)


# Check for customers with future signup dates
from pyspark.sql.functions import current_date

future_signups = df.filter(
    df.signup_date > current_date()
)

print("Customers with future signup dates:", future_signups.count())

future_signups.show()

# Check for invalid customer IDs
invalid_ids = df.filter(
    df.customer_id <= 0
)

print("Invalid customer IDs:", invalid_ids.count())

invalid_ids.show()

# 4. Filter customers from Hyderabad
hyderabad_customers = df.filter(
    df.city == "Hyderabad"
)

hyderabad_customers.show()

print(
    "Hyderabad customers:",
    hyderabad_customers.count()
)

# 5. Select specific columns
df.select("customer_id", "city").show()

# 6. Filter rows and select columns together
result = df.filter(
    df.city == "Hyderabad"
).select(
    "customer_id",
    "first_name",
    "city"
)

result.show()

# 7. Stop the Spark session
spark.stop()
