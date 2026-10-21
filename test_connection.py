import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

connection = psycopg2.connect(
    host=os.getenv("DB_HOST", "localhost"),
    database=os.getenv("DB_NAME", "ecommerce"),
    user=os.getenv("DB_USER", "postgres"),
    password=os.environ["DB_PASSWORD"],
    port=os.getenv("DB_PORT", "5432"),
)

print("Connected to PostgreSQL successfully!")
connection.close()