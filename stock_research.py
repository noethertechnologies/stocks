import os
import sys
import json
import requests
import streamlit as st
from dotenv import load_dotenv
import google.generativeai as genai
import psycopg2

# --- Configuration ---
load_dotenv()
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
model = genai.GenerativeModel('gemini-2.0-flash')

API_BASE_URL = "https://mgo2gncdj3.execute-api.ap-south-1.amazonaws.com"
EQUITY_DETAILS_ENDPOINT = f"{API_BASE_URL}/equity-details"

# Database configuration (PostgreSQL)
DB_HOST = "localhost"         # Replace with your DB host
DB_NAME = "stock_analytics"   # Replace with your DB name
DB_USER = "root"              # Replace with your DB user
DB_PASSWORD = "arka1256"      # Replace with your DB password

# --- Fetch Equity Details using requests ---
def fetch_equity_details(symbol):
    try:
        url = f"{EQUITY_DETAILS_ENDPOINT}?symbol={symbol}"
        st.write(f"Fetching equity details for **{symbol}** from `{url}`")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        st.error(f"Error fetching details for {symbol}: {e}")
        return None

# --- Use AI to Generate an Investment Research Report ---
def generate_research_report(symbol, equity_json):
    try:
        # Instruct the AI to extract key details and write a concise research report.
        prompt = (
            f"You are the best investment research analyst. "
            f"Below is the JSON data for the stock '{symbol}'. "
            "Extract the following key details: market capitalization, last price, sector PE, symbol PE, financial results, "
            "market department order book total buy quantity, total sell quantity, and security-wise delivery position (if available). "
            "Based on these data points, write a short, concise investment research report that includes a company overview, "
            "financial highlights, risk factors, and a recommendation summary.\n\n"
            f"{json.dumps(equity_json)}"
        )
        st.write(f"Generating research report for **{symbol}**...")
        response = model.generate_content(prompt)
        if response and response.text:
            return response.text.strip()
        else:
            return "No research report generated."
    except Exception as e:
        st.error(f"Error generating report for {symbol}: {e}")
        return "Error generating research report."

# --- Database: Create Research Report Table if Not Exists ---
def create_research_table(cur):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS investment_research (
        symbol VARCHAR(20) PRIMARY KEY,
        research_report TEXT
    );
    """
    cur.execute(create_table_sql)

# --- Insert Research Report into Database ---
def insert_research_report(cur, symbol, report):
    insert_sql = """
    INSERT INTO investment_research (symbol, research_report)
    VALUES (%s, %s)
    ON CONFLICT (symbol) DO UPDATE SET
      research_report = EXCLUDED.research_report;
    """
    cur.execute(insert_sql, (symbol, report))

# --- Main Functionality ---
def main():
    st.title("Stock Investment Research Report Generator")
    stock_symbol = st.text_input("Enter a Stock Symbol (e.g., AAPL):").strip().upper()
    
    if st.button("Generate Report") and stock_symbol:
        # Fetch equity details using requests
        equity_data = fetch_equity_details(stock_symbol)
        if not equity_data:
            return
        
        # Generate research report using AI
        report = generate_research_report(stock_symbol, equity_data)
        st.subheader("Investment Research Report")
        st.write(report)
        
        # Insert the research report into the database
        try:
            conn = psycopg2.connect(
                host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
            )
            cur = conn.cursor()
            create_research_table(cur)
            insert_research_report(cur, stock_symbol, report)
            conn.commit()
            st.success(f"Research report for **{stock_symbol}** inserted into database.")
        except Exception as e:
            st.error(f"Error inserting report into database: {e}")
        finally:
            try:
                cur.close()
                conn.close()
            except Exception as e:
                st.error(f"Error closing DB connection: {e}")

if __name__ == "__main__":
    main()
