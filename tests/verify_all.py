import subprocess
import os
from src.logger import setup_logger

logger = setup_logger()

# List of verification/test scripts to be executed
tests = [
    "verify_bronze.py",
    "verify_silver.py",
    "verify_gold.py",
    "check_duplicates_silver.py",
    "check_duplicates_gold.py",
    "test_transform.py",
    "check_eda_silver.py",
    "check_eda_gold.py",
    "test_gold_qualify.py"
]

logger.info("Starting verifications...")

for test in tests:
    logger.info(f"Running check: {test}")

    test_path = f"/home/project/tests/{test}"
    if not os.path.exists(test_path):
        logger.warning(f"[SKIP] File not found: {test_path}")
        continue

    # Execute the test script as a subprocess
    result = subprocess.run(
        ["python3", test_path],
        capture_output=True,
        text=True
    )

    # Check the result of the execution
    if result.returncode != 0:
        logger.error(f"Check failed: {test}")
        logger.error(result.stderr)
    else:
        logger.info(f"✅ Check OK: {test}")