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

# Get database connection
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

# Setup database
def setup_database():
    connection = None
    cursor = None
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
        # Close cursor and connection if they were created
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()
            print("Database setup complete.")

# Insert data into database
def insert_data_to_db(data):
    connection = get_db_connection()
    if not connection:
        return

    cursor = None
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
        # Close cursor and connection if they were created
        if cursor is not None:
            cursor.close()
        if connection.is_connected():
            connection.close()
            print("Database data insertion complete.")

# Check to see if the rsi and moving avg tables exist and if not add them   
def check_calculations_columns():

    connection = get_db_connection()
    if not connection:
        return

    try:
        cursor = connection.cursor()

        # Check if columns exist in the table
        cursor.execute("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'stock_data';
        """, (DB_NAME,))
        existing_columns = [row[0] for row in cursor.fetchall()]

        # Define the columns we need
        required_columns = {
            "rsi": "FLOAT NULL",
            "moving_average": "FLOAT NULL"
        }

        # Add missing columns
        for column_name, column_definition in required_columns.items():
            if column_name not in existing_columns:
                print(f"Column '{column_name}' does not exist. Adding it...")
                cursor.execute(f"ALTER TABLE stock_data ADD COLUMN {column_name} {column_definition};")
                print(f"Column '{column_name}' added successfully.")

        connection.commit()

    except Error as e:
        print(f"Error ensuring columns exist: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection.is_connected():
            connection.close()

# adding a way to update columns as a function to make it easier to incorporate data into the mysql database 
def update_column(data, column_name):

    connection = get_db_connection()
    if not connection:
        return

    try:
        cursor = connection.cursor()

        # Validate if the column exists
        cursor.execute("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'stock_data' AND COLUMN_NAME = %s;
        """, (DB_NAME, column_name))
        if not cursor.fetchone():
            raise ValueError(f"Column '{column_name}' does not exist in the 'stock_data' table.")

        # Prepare the SQL query
        query = f"""
        INSERT INTO stock_data (date, symbol, {column_name})
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE {column_name} = VALUES({column_name});
        """

        # Insert data row by row
        for row in data:
            cursor.execute(query, (row["date"], row["symbol"], row[column_name]))

        # Commit changes
        connection.commit()
        print(f"Updated {len(data)} rows in column '{column_name}'.")

    except Error as e:
        print(f"Error updating column '{column_name}': {e}")
    finally:
        if cursor:
            cursor.close()
        if connection.is_connected():
            connection.close()
