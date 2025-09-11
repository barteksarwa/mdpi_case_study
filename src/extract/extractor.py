import os
import glob
import json
import requests
from datetime import datetime
from logging import Logger
from requests.exceptions import HTTPError
from tqdm import tqdm
import time 

from src.utils.config import Config

class Extractor:
    """
    DataLoader class for loading data from CrossRef API.

    Attributes:
        config (Config): Configuration object containing API endpoint and key.
        headers (Dict[str, str]): Headers for the API request.
    """

    def __init__(self, config: Config, logger: Logger):
        self.config = config
        self.headers = {
            "Accept": "application/json",
        }

        self.logger = logger
        self.logger.info("DataLoader initialized with config: %s", config.__dict__)
        self.logger.info("Headers set for API request: %s", self.headers)
        self.logger.info("DataLoader initialized successfully.")


    # todo - function to fetch data from CrossRef API by looping through the pages
    def fetch_and_save_data(self, target_items=200):
        self.logger.info("Starting data fetch from CrossRef API...")
        
        # Initialize variables for pagination
        cursor = "*"
        total_items = 0
        page = 1
        base_url = self.config.api_endpoint
        
        # Create a progress bar
        progress_bar = tqdm(desc="Fetching data", unit="page")
        
        while total_items < target_items:
            # Build URL with cursor for pagination
            url = f"{base_url}&cursor={cursor}"
            self.logger.info(f"Fetching page {page} with URL: {url}")
            
            try:
                # Make API request
                self.logger.debug(f"Making request to: {url}")
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()  # Raise an error for bad responses
                data = response.json()
                self.logger.info(f"Page {page} fetched successfully. Status code: {response.status_code}")
            except HTTPError as http_err:
                self.logger.error(f"HTTP error occurred: {http_err}")
                break
            except Exception as err:
                self.logger.error(f"An error occurred: {err}")
                break

            # Extract items from response
            items = data.get("message", {}).get("items", [])
            num_items = len(items)
            total_items += num_items
            
            # Update progress bar
            progress_bar.set_postfix({
                "page": page,
                "items_this_page": num_items,
                "total_items": total_items
            })
            progress_bar.update(1)
            
            self.logger.info(f"Page {page} has {num_items} items. Total items so far: {total_items}")
            
            # Save raw JSON response to file
            now = datetime.now()
            filename = now.strftime("%Y%m%d_%H%M%S") + f"_page_{page}.json"
            filepath = f"./data/raw/{filename}"
            
            try:
                with open(filepath, "w") as f:
                    json.dump(data, f, indent=4)
                self.logger.info(f"Saved raw data to {filepath}")
            except Exception as e:
                self.logger.error(f"Failed to save data to {filepath}: {e}")

            # Check if we have enough items
            if total_items >= target_items:
                self.logger.info(f"Reached target of {target_items} items (actual: {total_items}). Stopping.")
                break

            # Get next cursor for pagination
            cursor = data.get("message", {}).get("next-cursor")
            if not cursor:
                self.logger.info("No more pages available. Stopping.")
                break

            page += 1
            
            # Add a small delay to be respectful to the API
            time.sleep(0.5)

        # Close the progress bar
        progress_bar.close()
        
        self.logger.info(f"Data fetch completed. Total items fetched: {total_items}")
        self.logger.info(f"Raw data saved to ./data/raw/ directory with {page} page(s) of data")

    def extract_raw_data(self):
        """
        Extract raw data from the json files in ./data/raw directory.
        """
        self.logger.info("Extracting raw data from JSON files...")

        data = []
        json_files = glob.glob(os.path.join("./data/raw", "*.json"))
        for file in tqdm(json_files, desc="Extracting data", unit="file"):
            with open(file, "r") as f:
                try:
                    json_data = json.load(f)

                    # only get json_data.items
                    if "message" in json_data and "items" in json_data["message"]:
                        items = json_data.get("message", {}).get("items", [])
                        for item in items:
                            if isinstance(item, dict):
                                data.append(item)
                            else:
                                self.logger.warning(f"Item is not a dictionary: {item}")
                    else:
                        self.logger.warning(f"No 'message' or 'items' found in file {file}")
                except json.JSONDecodeError as e:
                    self.logger.error(f"Error decoding JSON from file {file}: {e}")
                    continue

        self.logger.info("Raw data extraction completed.")
        return data
