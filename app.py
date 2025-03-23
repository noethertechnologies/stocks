import os
import json
import time
import psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# Constants
SYMBOLS_URL = "http://localhost:5000/symbols"
EQUITY_DETAILS_URL = "http://localhost:5000/equity-details"

# Database connection settings (modify with your credentials)
DB_HOST = "localhost"
DB_NAME = "equity_data"
DB_USER = "root"
DB_PASSWORD = "arka1256"

# Function to create Selenium WebDriver instance
def create_webdriver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
    print("WebDriver initialized.")
    return driver

# Function to connect to the PostgreSQL database
def connect_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Connected to the database.")
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

# Function to create table for storing equity data if it doesn't exist
def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS equity_data (
                symbol VARCHAR(10) PRIMARY KEY,
                data JSONB
            )
        """)
        conn.commit()
        print("Table created or already exists.")

# Function to fetch symbols
def fetch_symbols(driver):
    print(f"Accessing symbols URL: {SYMBOLS_URL}")
    driver.get(SYMBOLS_URL)
    time.sleep(2)  # Wait for the page to load

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    pre = soup.find('pre')

    if pre:
        symbols_data = json.loads(pre.text)
        symbols = symbols_data.get("symbols", [])
        print(f"Fetched {len(symbols)} symbols.")
        return symbols
    else:
        print("No symbols found.")
        return []

# Function to fetch and insert equity details into the database for each symbol
def fetch_and_store_equity_details(symbol, driver, conn):
    try:
        equity_details_url = f"{EQUITY_DETAILS_URL}?symbol={symbol}"
        print(f"Fetching equity details for {symbol} from {equity_details_url}")
        driver.get(equity_details_url)
        time.sleep(1)  # Wait for the page to load

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        pre = soup.find('pre')

        if pre:
            equity_details = json.loads(pre.text)

            # Insert data into PostgreSQL database
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO equity_data (symbol, data)
                    VALUES (%s, %s)
                    ON CONFLICT (symbol) DO UPDATE SET data = EXCLUDED.data
                """, (symbol, json.dumps(equity_details)))
                conn.commit()

            print(f"Data for {symbol} inserted into the database.")
        else:
            print(f"No data found for symbol: {symbol}.")
    except Exception as e:
        print(f"Error fetching equity details for {symbol}: {e}")

# Main function to fetch symbols and their equity details
def main():
    driver = create_webdriver()
    conn = connect_db()
    
    if conn is None:
        print("Database connection failed. Exiting...")
        return

    try:
        # Create table if it doesn't exist
        create_table(conn)

        # Fetch all symbols
        symbols = fetch_symbols(driver)

        # Fetch and store equity details for each symbol
        if symbols:
            print("Starting to fetch equity details for each symbol...")
            for symbol in symbols:
                fetch_and_store_equity_details(symbol, driver, conn)
        else:
            print("No symbols to process.")
    finally:
        driver.quit()
        conn.close()
        print("WebDriver closed. Database connection closed.")

if __name__ == "__main__":
    main()
