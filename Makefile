# Default variables
data_process=$(shell date +%Y-%m-%d)
LOAD_MODE=delta
DELTA_DAYS=2
PYTHON=python3
PYTHON_CMD=PYTHONPATH=./ $(PYTHON)
PYTHONPATH=./

# Main script paths
BRONZE=./scripts/exec_bronze.py
SILVER=./scripts/exec_silver.py
GOLD=./scripts/exec_gold.py
VERIFY=./scripts/verify_all.py

# Test/validation scripts
TEST_TRANSFORM=tests/test_transform.py
CHECK_DUPLICATES_SILVER=tests/check_duplicates_silver.py
CHECK_DUPLICATES_GOLD=tests/check_duplicates_gold.py

# Spark container (adjust if necessary)
CONTAINER=spark-container

# Local execution (recommended outside container)
bronze:
    $(PYTHON_CMD) $(BRONZE)

silver:
    $(PYTHON_CMD) $(SILVER)

gold:
    $(PYTHON_CMD) $(GOLD)

verify:
    $(PYTHON_CMD) $(VERIFY)

test:
    pytest $(TEST_TRANSFORM)

duplicates-silver:
    $(PYTHON_CMD) $(CHECK_DUPLICATES_SILVER)

duplicates-gold:
    $(PYTHON_CMD) $(CHECK_DUPLICATES_GOLD)


# Docker execution (using PYTHONPATH=/home/project)

verify-all:
    docker exec -e PYTHONPATH=/home/project -it spark-container \
    python3 /home/project/tests/verify_all.py

# Run everything in sequence
all: bronze silver gold verify
