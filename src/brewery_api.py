import os, math, json, time, requests
from datetime import datetime
from src.logger import CustomLogger

logger_instance = CustomLogger()
logger = logger_instance.get_logger()

class BreweryDataExtractor:
    """
    A class to extract brewery data from the Open Brewery DB API.

    This class handles fetching metadata to determine the total number of pages,
    iterating through each page, and saving the results as individual JSON files.
    It also includes logic for retries and delays.
    """

    def __init__(
        self,
        base_url: str = "https://api.openbrewerydb.org/v1/breweries",
        meta_url: str = "https://api.openbrewerydb.org/v1/breweries/meta",
        output_dir: str = "data/bronze",
        per_page: int = 50,
        request_delay: float = 1.0,
        max_retries: int = 5,
        timeout: int = 10
    ):        
        self.base_url = base_url
        self.meta_url = meta_url
        self.output_dir = output_dir
        self.per_page = per_page
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.timeout = timeout
        self.logger = logger

    def _get_metadata(self) -> dict | None:
        """
        Fetches metadata from the API to get the total number of breweries.

        Returns:
            dict | None: A dictionary containing the metadata, or None if the request fails.
        """
        try:
            self.logger.info("Fetching API metadata...")
            response = requests.get(self.meta_url, timeout=self.timeout)
            response.raise_for_status()  # Raise an exception for HTTP error codes
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching API metadata: {e}")
            return None

    def _fetch_page(self, page_number: int) -> list | None:
        """
        Fetches a single page of brewery data from the API.

        Args:
            page_number (int): The page number to fetch.

        Returns:
            list | None: A list of brewery records, or None if the request fails.
        """
        url = f"{self.base_url}?per_page={self.per_page}&page={page_number}"
        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching page {page_number}: {e}")
            return None

    def _save_data(self, data: list, page_number: int, date_process: str) -> None:

        # Create a subdirectory for the given processing date
        date_specific_dir = os.path.join(self.output_dir, date_process)
        os.makedirs(date_specific_dir, exist_ok=True)

        # Define the output filename
        file_path = os.path.join(date_specific_dir, f"breweries_pag_{page_number}.json")

        # Write data to the JSON file
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            # self.logger.info(f"Successfully saved data to {file_path}")
        except IOError as e:
            self.logger.error(f"Failed to write to file {file_path}: {e}")

    def run_extraction(self, date_process: str = None) -> None:
        """
        Executes the full data extraction process.
        It fetches the total number of records, calculates the number of pages,
        and then iterates through each page to fetch and save the data.
        """
        self.logger.info(f"Starting brewery data extraction from API: {self.base_url}")

        # Determine the processing date
        if date_process is None:
            date_process = datetime.now().strftime("%Y-%m-%d")

        metadata = self._get_metadata()
        if not metadata or 'total' not in metadata:
            self.logger.error("Could not retrieve total count from metadata. Aborting.")
            return

        total_records = int(metadata['total'])
        total_pages = math.ceil(total_records / self.per_page)
        self.logger.info(f"Total records in API: {total_records}")
        self.logger.info(f"Total pages to fetch: {total_pages} ({self.per_page} records per page).")

        consecutive_failures = 0
        total_pages_saved = 0
        total_records_saved = 0
        current_page = 1

        while current_page <= total_pages:
            data = self._fetch_page(current_page)

            if data:
                consecutive_failures = 0
                self._save_data(data, current_page, date_process)
                
                total_pages_saved += 1
                total_records_saved += len(data)

                current_page += 1
                time.sleep(self.request_delay)
            else:
                consecutive_failures += 1
                self.logger.warning(f"Consecutive failures: {consecutive_failures}")

                if consecutive_failures >= self.max_retries:
                    self.logger.error(
                        f"Reached maximum of {self.max_retries} consecutive failures. Aborting extraction."
                    )
                    break
                
                time.sleep(self.request_delay * 5)
        
        self.logger.info("Bronze layer extraction finished.")
        self.logger.info(f"Summary: {total_pages_saved} pages saved, {total_records_saved} records saved.")

extractor = BreweryDataExtractor()