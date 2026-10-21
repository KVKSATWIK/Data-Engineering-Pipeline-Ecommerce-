
import subprocess
import sys

steps = [
    ("Transform customers", "src/processing/transform_customers.py"),
    ("Transform products", "src/processing/transform_products.py"),
    ("Transform orders", "src/processing/transform_orders.py"),
    ("Transform order items", "src/processing/transform_order_items.py"),
    ("Transform payments", "src/processing/transform_payments.py"),
    ("Validate processed tables", "src/processing/validate_all_tables.py"),
    ("Build analytics dataset", "src/processing/build_order_line_dataset.py"),
    ("Validate analytics dataset", "src/processing/validate_analytics_dataset.py"),
    ("Validate order/payment consistency", "src/processing/validate_order_payment.py"),
]

for name, script in steps:
    print(f"\n{'=' * 60}")
    print(f"RUNNING: {name}")
    print(f"{'=' * 60}", flush=True)

    result = subprocess.run([sys.executable, script])

    if result.returncode != 0:
        print(f"\nFAILED: {name}. Pipeline stopped.")
        sys.exit(result.returncode)

print("\nPIPELINE COMPLETED SUCCESSFULLY!")
