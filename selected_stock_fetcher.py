import os
import json
import time
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# Constants
API_BASE_URL = "http://localhost:5000"
EQUITY_DETAILS_ENDPOINT = f"{API_BASE_URL}/equity-details"
SYMBOLS_ENDPOINT = f"{API_BASE_URL}/symbols"
OUTPUT_DIR = "selected_stocks"  # Directory to store fetched equity details
os.makedirs(OUTPUT_DIR, exist_ok=True)  # Create output directory if it doesn't exist

# Function to create Selenium WebDriver instance
def create_webdriver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(
        service=ChromeService(ChromeDriverManager().install()), options=chrome_options
    )
    print("WebDriver initialized.")
    return driver

# Function to fetch all stock symbols from the API
def fetch_all_symbols():
    try:
        print(f"Fetching all stock symbols from {SYMBOLS_ENDPOINT}")
        response = requests.get(SYMBOLS_ENDPOINT)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        symbols = data.get("symbols", [])
        print(f"Fetched {len(symbols)} symbols.")
        return symbols
    except Exception as e:
        print(f"Error fetching stock symbols: {e}")
        return []

# Function to fetch and save equity details as JSON files
def fetch_and_store_equity_details(symbol, driver):
    try:
        equity_details_url = f"{EQUITY_DETAILS_ENDPOINT}?symbol={symbol}"
        print(f"Fetching equity details for {symbol} from {equity_details_url}")
        driver.get(equity_details_url)
        time.sleep(1)  # Wait for the page to load

        soup = BeautifulSoup(driver.page_source, "html.parser")
        pre = soup.find("pre")

        if pre:
            equity_details = json.loads(pre.text)

            # Define the JSON file path
            file_path = os.path.join(OUTPUT_DIR, f"{symbol}.json")

            # Write the fetched details to a JSON file
            with open(file_path, "w") as json_file:
                json.dump(equity_details, json_file, indent=4)
            print(f"Data for {symbol} saved to {file_path}.")
        else:
            print(f"No data found for symbol: {symbol}.")
    except Exception as e:
        print(f"Error fetching or saving data for {symbol}: {e}")

# Main function to fetch all symbols and their equity details
def main():
    driver = create_webdriver()

    try:
        # Fetch all stock symbols
        symbols = fetch_all_symbols()

        if not symbols:
            print("No symbols fetched. Exiting.")
            return

        print(f"Starting to fetch and save equity details for {len(symbols)} symbols...")
        for symbol in symbols:
            fetch_and_store_equity_details(symbol, driver)

        print("Data fetching and saving completed successfully.")
    finally:
        driver.quit()
        print("WebDriver closed.")

if __name__ == "__main__":
    main()
