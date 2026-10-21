
import subprocess
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent

steps = [
    (
        "Extract PostgreSQL data",
        [sys.executable, "src/ingestion/extract_postgres.py"],
    ),
    (
        "Validate raw CSV files",
        [sys.executable, "src/ingestion/validate_raw.py"],
    ),
]

for name, command in steps:
    print(f"\n{'=' * 60}\n{name}\n{'=' * 60}", flush=True)

    result = subprocess.run(command, cwd=PROJECT_DIR)

    if result.returncode != 0:
        print(f"\nFAILED: {name}. Pipeline stopped.")
        sys.exit(result.returncode)

print("\nStarting PySpark processing in WSL...", flush=True)

wsl_command = (
    "cd /mnt/c/ecommerce-de && "
    "/home/satwi/ecommerce-venv/bin/python run_processing.py"
)

result = subprocess.run(
    ["wsl.exe", "--exec", "bash", "-lc", wsl_command],
    cwd=PROJECT_DIR,
)

if result.returncode != 0:
    print("\nFAILED: WSL processing. Pipeline stopped.")
    sys.exit(result.returncode)

print("\nFULL PIPELINE COMPLETED SUCCESSFULLY!")
