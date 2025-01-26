import requests
import polars as pl
from datetime import datetime
from dotenv import load_dotenv
import os

# Import the db_handler functions
from db_handler import setup_database, insert_data_to_db

# Load environment variables
load_dotenv()

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
BASE_URL = "https://www.alphavantage.co/query"

# List of stock symbols to query
STOCK_SYMBOLS = ["NVDA"]

# Step 1: Ensure the database is ready
setup_database()

# Step 2: Function to fetch data from Alpha Vantage
def fetch_stock_data(symbol):
    try:
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": ALPHA_VANTAGE_API_KEY
        }
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Check if the response contains valid data
        if "Time Series (Daily)" not in data:
            print(f"Error fetching data for {symbol}: {data.get('Note', 'Unknown error')}")
            return []

        time_series = data["Time Series (Daily)"]
        rows = []
        
        for date, metrics in time_series.items():
            row = {
                "date": datetime.strptime(date, "%Y-%m-%d").date(),
                "symbol": symbol,
                "open": float(metrics["1. open"]),
                "high": float(metrics["2. high"]),
                "low": float(metrics["3. low"]),
                "close": float(metrics["4. close"]),
                "volume": int(metrics["5. volume"]),
            }
            rows.append(row)
        
        return rows

    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return []

# Step 3: Loop through the stocks and fetch + insert data
def main():
    for symbol in STOCK_SYMBOLS:
        print(f"Fetching data for {symbol}...")
        stock_data = fetch_stock_data(symbol)
        
        if stock_data:
            print(f"Inserting data for {symbol} into the database...")
            insert_data_to_db(stock_data)
        else:
            print(f"No data to insert for {symbol}.")

if __name__ == "__main__":
    main()