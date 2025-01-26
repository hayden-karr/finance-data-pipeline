import polars as pl
from ta.momentum import RSIIndicator
from ta.trend import SMAIndicator

# Import the db_handler functions
from db_handler import get_db_connection, update_column


# Connect to the database
connection = get_db_connection()

def calculate_and_update_rsi(symbol, window=14):
    connection = get_db_connection()
    if not connection:
        print("Failed to connect to the database.")
        return

    try:
        # Query the database and load data into a Polars DataFrame
        query = "SELECT date, close FROM stock_data WHERE symbol = %s ORDER BY date ASC;"
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, (symbol,))
        result = cursor.fetchall()

        if not result:
            print(f"No data found for symbol: {symbol}")
            return

        data = pl.DataFrame(result)

        # Convert to Pandas for RSI calculation
        data_pandas = data.to_pandas()
        rsi_indicator = RSIIndicator(close=data_pandas["close"], window=window)
        data_pandas["rsi"] = rsi_indicator.rsi()

        # Convert back to Polars
        data_with_rsi = pl.from_pandas(data_pandas)

        # Prepare data for database update
        rsi_data = data_with_rsi.select(["date", "rsi"]).drop_nulls().to_dicts()
        for row in rsi_data:
            row["symbol"] = symbol  # Add the symbol to each row

        # Update the database using the update_column function
        update_column(rsi_data, "rsi")
        print(f"RSI calculation and update complete for symbol: {symbol}")

    except Exception as e:
        print(f"Error during RSI calculation for symbol {symbol}: {e}")
    finally:
        if connection.is_connected():
            connection.close()


# Function to find the moving average of given stock data
def calculate_moving_avg(symbol,window=14):
        connection = get_db_connection()

        if not connection:
            print("Failed to connect to the database.")
            return

        try:
            # Query the database and insert data into polars dataframe
            query = "SELECT date, close from stock_data  WHERE symbol = %s ORDER BY date ASC;"
            cursor = connection.cursor(dictionary=True)
            cursor.execute(query, (symbol,))
            result = cursor.fetchall()

            if not result:
                print(f"No data was found for {symbol}")
                return
        
            data = pl.DataFrame(result)

            # Calculate using the ta library for the sma and using convert to pandas
            data_pandas = data.to_pandas()
            sma_moving_avg = SMAIndicator(close=data_pandas['close'],window=window)
            data_pandas["moving_average"] = sma_moving_avg.sma_indicator()

            # Convert back to Polars
            data_moving_avg = pl.from_pandas(data_pandas)

            # Prep data for database insert
            moving_avg_data = data_moving_avg.select(["date", "moving_average"]).drop_nulls().to_dicts()
            for row in moving_avg_data:
                row["symbol"] = symbol
            
            # Update the database using the update_column function
            update_column(moving_avg_data, "moving_average")
            print(f"Moving average calculation and update complete for symbol: {symbol}")

        except Exception as e:
            print(f"Error during moving average for symbol {symbol}: {e}")
        finally:
            if connection.is_connected():
                connection.close()

