import os

import pandas as pd
from sqlalchemy import create_engine


engine = create_engine(
    "postgresql+psycopg2://postgres:satwikpostgres@localhost:5432/ecommerce"
)

tables = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments"
]

os.makedirs("data/raw", exist_ok=True)

for table in tables:

    query = f"SELECT * FROM {table}"

    df = pd.read_sql(query, engine)

    file_path = f"data/raw/{table}.csv"

    df.to_csv(file_path, index=False)

    print(f"Extracted {len(df)} rows from {table}")


print("Extraction completed successfully!")