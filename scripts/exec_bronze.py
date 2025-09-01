import os
from datetime import datetime
from src.brewery_api import BreweryDataExtractor 
from src.logger import CustomLogger 

BRONZE_OUTPUT_DIR = os.path.join("data", "bronze")

logger_instance = CustomLogger()
logger = logger_instance.get_logger()

def main():
    """
    Orchestrates the data extraction from the Brewery API into the Bronze layer.
    
    This function initializes the data extractor, sets the processing date,
    and runs the extraction process, logging the progress and any errors.
    """
    logger.info("Starting Bronze layer extraction process...")
    
    try:
        date_process = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"Processing date set to: {date_process}")

        extractor = BreweryDataExtractor(
            output_dir=BRONZE_OUTPUT_DIR
        )
        
        extractor.run_extraction(date_process=date_process)
        
        logger.info("Bronze layer extraction completed successfully.")

    except Exception as e:
        logger.exception(f"An error occurred during the Bronze layer execution: {e}")

if __name__ == "__main__":
    main()
