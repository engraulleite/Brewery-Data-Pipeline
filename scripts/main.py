from src.logger import CustomLogger
from scripts import exec_bronze, exec_gold, exec_silver

logger_instance = CustomLogger()
logger = logger_instance.get_logger()

if __name__ == "__main__":
    try:
        logger.info("Starting full pipeline (Bronze → Silver → Gold)")
        exec_bronze.main()
        exec_silver.main()
        exec_gold.main()
        logger.info("Pipeline executed successfully.")
    except Exception as e:
        logger.error(f"Error during full pipeline execution: {e}")
        raise
