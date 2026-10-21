
import os
import random

import psycopg2
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

fake = Faker("en_IN")

connection = psycopg2.connect(
    host=os.getenv("DB_HOST", "localhost"),
    database=os.getenv("DB_NAME", "ecommerce"),
    user=os.getenv("DB_USER", "postgres"),
    password=os.environ["DB_PASSWORD"],
    port=os.getenv("DB_PORT", "5432"),
)

cursor = connection.cursor()

# Get existing orders
cursor.execute("""
    SELECT order_id, order_date, total_amount
    FROM orders;
""")

orders = cursor.fetchall()

payment_methods = [
    "UPI",
    "Credit Card",
    "Debit Card",
    "Net Banking",
    "Cash on Delivery"
]

payment_statuses = [
    "Completed",
    "Pending",
    "Failed",
    "Refunded"
]

for order_id, order_date, total_amount in orders:
    payment_method = random.choice(payment_methods)
    payment_status = random.choice(payment_statuses)

    cursor.execute(
        """
        INSERT INTO payments
        (order_id, payment_date, payment_method, payment_status, amount)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            order_id,
            order_date,
            payment_method,
            payment_status,
            total_amount
        )
    )

connection.commit()

cursor.close()
connection.close()

print("Payments inserted successfully!")
