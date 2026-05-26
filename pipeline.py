"""
Jaffle Shop Data Pipeline.
Optimized for high performance using chunking, multiprocessing, and file rotation.
"""
import os
import logging
from typing import Iterator, Dict, Any, List

import dlt
import requests

# --- Configuration & Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://api.jaffle-shop.com"
ENDPOINTS = ["customers", "orders", "products"]

# --- Performance Tuning via Environment Variables ---
# In production, these should ideally be set in config.toml or via secret managers.
os.environ["EXTRACT__WORKERS"] = "3"
os.environ["NORMALIZE__WORKERS"] = "3"
os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "50000"
os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "100000"

@dlt.resource(write_disposition="replace", parallelized=True)
def fetch_jaffle_data(endpoint: str) -> Iterator[List[Dict[str, Any]]]:
    """
    Fetches paginated data from the specified Jaffle Shop API endpoint.
    Leverages chunking by yielding entire pages instead of individual items 
    to reduce Python's GIL overhead during the extract phase.

    Args:
        endpoint (str): The specific API endpoint to extract (e.g., 'customers').

    Yields:
        Iterator[List[Dict[str, Any]]]: A list of dictionary records representing a page.
    """
    url = f"{BASE_URL}/{endpoint}"
    page = 1

    while True:
        logger.info(f"Extracting {endpoint} - Page {page}")
        try:
            response = requests.get(url, params={"page": page}, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch data from {url}: {e}")
            break

        data = response.json()
        
        # Extract items safely, handling potential wrapper objects
        items = data.get("data", data) if isinstance(data, dict) else data

        if not items:
            logger.info(f"End of pagination reached for {endpoint}.")
            break

        # Yield the entire list at once (Chunking)
        yield items 
        page += 1

@dlt.source
def jaffle_shop_source() -> Any:
    """
    Groups all Jaffle Shop resources into a single source for parallel execution.
    """
    for endpoint in ENDPOINTS:
        yield fetch_jaffle_data(endpoint).with_name(endpoint)

if __name__ == "__main__":
    logger.info("Initializing highly optimized Jaffle Shop pipeline...")
    
    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop",
        destination="duckdb",
        dataset_name="jaffle_data"
    )
    
    # Execute the pipeline
    load_info = pipeline.run(jaffle_shop_source())
    
    logger.info("Pipeline executed successfully.")
    logger.info(load_info)
