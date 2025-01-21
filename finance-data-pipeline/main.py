import polars as pl
import requests
import os
import json
import time 
from datetime import datetime, timedelta, timezone
from key import alpha_vantage_api_key


# alpha vantage api limitations - 25 calls per day
# example for api url call for reference - https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo
# alpha vantage doc - https://www.alphavantage.co/documentation/

base_url = "https://www.alphavantage.co/query"
calls_tracker_file = 'finance-data-pipeline/api_calls_tracker.json'

#create tracking json file to account for 25 calls per day
def load_api_calls_tracker():
    if os.path.exists(calls_tracker_file):
        with open(calls_tracker_file, 'r') as f:
            return json.load(f)
    else:
        return {'date': str(datetime.now(timezone.utc)),'calls': 0}
    
def save_api_calls_tracker(tracker):
    with open(calls_tracker_file, 'w') as f:
        json.dump(tracker, f)


# Function to reset tracker for a new day
def reset_api_calls_tracker(tracker):
    tracker["date"] = str(datetime.now(timezone.utc))
    tracker["calls"] = 0
    save_api_calls_tracker(tracker)


#function to fetch stock data and load into polars dataframe
# def fetch_to_polars(symbol):
#     try: 
#         params = {
#             'function': 'TIME_SERIES_MONTHLY',
#             'symbol': symbol,
#             'apikey': alpha_vantage_api_key,
#             'datatype': 'json',
#         }

#         response = requests.get(base_url,params=params)

#         #raise http error for a bad status code
#         response.raise_for_status()

#         data = response.json()

#         #extract time series daily data
#         time_series = data.get('Time series (Monthly)',{})


#         if not time_series:
#             raise ValueError(f'No time series data found for {symbol}')

#         #transform data in json format to dictionaries
#         rows = []
#         for date, metrics in time_series.items():
#             row = {'date': date}
#             row.update({
#                 'open': float(metrics['open']),
#                 'high': float(metrics['high']),
#                 'low': float(metrics['low']),
#                 'close': float(metrics['close']),
#                 'volume': int(metrics['volume'])
#             })

#             rows.append(row)
        
#         #create polars data frame
#         df = pl.DataFrame(rows)
#         return df
    
#     except Exception as ex:
#         print(f'Error fetching data for {symbol}: {ex}')
#         return None



def fetch_stock_data(symbol):
    try:
        # Request data from the API
        params = {
            "function": "TIME_SERIES_MONTHLY",  # Use the same function as your test
            "symbol": symbol,
            "apikey": alpha_vantage_api_key,
            "datatype": "json",
        }
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors

        # Parse the JSON response
        data = response.json()

        # Check for the time series key
        time_series = data.get("Monthly Time Series", {})
        if not time_series:
            raise ValueError(f"No time series data found for {symbol}.")

        # Convert the time series data into rows
        rows = []
        for date, metrics in time_series.items():
            row = {
                "date": date,
                "open": float(metrics["1. open"]),
                "high": float(metrics["2. high"]),
                "low": float(metrics["3. low"]),
                "close": float(metrics["4. close"]),
                "volume": int(metrics["5. volume"]),
            }
            rows.append(row)

        # Create a Polars DataFrame
        df = pl.DataFrame(rows)
        return df

    except requests.exceptions.RequestException as e:
        print(f"Request error fetching data for {symbol}: {e}")
    except ValueError as e:
        print(f"Error for {symbol}: {e}")
    except Exception as e:
        print(f"Unexpected error for {symbol}: {e}")
    return None
    
#function to fetch multiple different stocks
def fetch_multiple_stocks(symbols, tracker):
    all_data = {}
    for symbol in symbols:
        #alpha vantage limit is 25 calls a day so ensure that their are remaining calls for the current date
        if tracker['calls'] >= 25:
            print('Max api calls for current date')
            break

        print(f'Collecting data for {symbol}')
        df = fetch_stock_data(symbol)

        tracker['calls'] += 1
        save_api_calls_tracker(tracker)

        #5 calls a minute limit so 60/5 = 12 seconds
        time.sleep(12)
    return all_data

if __name__ == '__main__':
   
    #load different stock symbols
    stock_symbols = ['NVDA','AMD']

    
    while True:

        #check to see if its a new date and so tracker can reset to 25 new calls
        tracker = load_api_calls_tracker()

        if tracker['date'] != str(datetime.now(timezone.utc)):
            reset_api_calls_tracker(tracker)
    
        #fetch data
        stock_data = fetch_multiple_stocks(stock_symbols, tracker)

        #save data to csvs
        os.makedirs('data',exist_ok=True)
        for symbol, df in stock_data.items():
            csv_path = f'finance-data-pipeline/data/{symbol}_daily.csv'
            df.write_csv(csv_path)
            print(f'Saved {symbol} to CSV')
        
        #check if more api calls can be made i.e. maxed out at 25
        if tracker['calls'] >= 25:
            print('No more calls today.')
            now = datetime.now(timezone.utc)
            reset_time = (now+ timedelta(days=1)).replace(hour=0,minute=0,second=0,microsecond=0)
            sleep_time = (reset_time - now).total_seconds()
            time.sleep(sleep_time)