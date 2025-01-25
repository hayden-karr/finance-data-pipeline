import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

def get_db_connection():
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        return connection
    except Error as e:
        print(f"Error connecting to the database: {e}")
        return None

def setup_database():
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD
        )
        if connection.is_connected():
            print("Connected to MySQL server")

            cursor = connection.cursor()

            # Create database if it doesn't exist
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
            print(f"Database '{DB_NAME}' is ready.")

            # Switch to the database
            connection.database = DB_NAME

            # Create table if it doesn't exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_data (
                date DATE NOT NULL,
                symbol VARCHAR(10) NOT NULL,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT,
                PRIMARY KEY (date, symbol)
            );
            """)
            print("Table 'stock_data' is ready.")

    except Error as e:
        print(f"Error setting up the database: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Database setup complete.")

def insert_data_to_db(data):
    connection = get_db_connection()
    if not connection:
        return

    try:
        cursor = connection.cursor()

        for row in data:
            try:
                cursor.execute("""
                INSERT INTO stock_data (date, symbol, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    open=VALUES(open),
                    high=VALUES(high),
                    low=VALUES(low),
                    close=VALUES(close),
                    volume=VALUES(volume);
                """, (row["date"], row["symbol"], row["open"], row["high"], row["low"], row["close"], row["volume"]))
            except Error as e:
                print(f"Error inserting row {row}: {e}")

        connection.commit()
        print(f"Inserted {len(data)} rows into the database.")

    except Error as e:
        print(f"Error inserting data: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
