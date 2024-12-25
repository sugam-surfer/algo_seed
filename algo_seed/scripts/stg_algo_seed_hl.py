import sys
from pathlib import Path
from datetime import datetime
import logging
import argparse
from psycopg2 import sql
from dateutil.relativedelta import relativedelta

# Add parent directory to sys.path for imports
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

from logger import setup_logger
from db_utils import get_db_connection
from utils import chunk_list
from config import load_config

# Constants
BATCH_SIZE = 5000
SOURCE_TABLE = "raw_algo_seed"
DESTINATION_TABLE = "stg_algo_seed_hl"
DESTINATION_COLUMNS = [
    "inserted_at", "ticker", "trade_date", "close_price", "volume",
    "w52_high", "w52_high_date", "w52_low", "w52_low_date", "is_boh_ok", "is_ath_ok"
]


# Add this function definition above or import it from utils if already present there.
def get_column_type(column_name):
    column_type_mapping = {
        "inserted_at": "TIMESTAMP",
        "ticker": "VARCHAR(50)",
        "trade_date": "DATE",
        "close_price": "NUMERIC",
        "volume": "NUMERIC",
        "w52_high": "NUMERIC",
        "w52_high_date": "DATE",
        "w52_low": "NUMERIC",
        "w52_low_date": "DATE",
        "is_boh_ok": "BOOLEAN",
        "is_ath_ok": "BOOLEAN"
    }
    # Default to a generic type if not found
    return column_type_mapping.get(column_name, "VARCHAR(255)")

# Function to create the table and indexes
def create_table_and_indexes(connection, logger):
    """Creates the destination table and associated indexes."""
    try:
        with connection.cursor() as cursor:
            column_definitions = ", ".join([
                f"{col} {get_column_type(col)}" for col in DESTINATION_COLUMNS
            ])
            unique_constraint = f"CONSTRAINT pk_{DESTINATION_TABLE} PRIMARY KEY (ticker, trade_date)"

            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {DESTINATION_TABLE} (
                {column_definitions},
                {unique_constraint}
            );
            """
            cursor.execute(create_table_query)

            # Create indexes
            for col in ["ticker", "trade_date", "inserted_at"]:
                cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{DESTINATION_TABLE}_{col} ON {DESTINATION_TABLE} ({col});
                """)

            connection.commit()
            logger.info(f"Table {DESTINATION_TABLE} and indexes created or already exist.")
    except Exception as e:
        logger.error(f"Error creating table or indexes: {e}")
        raise

# Fetch the latest inserted_at timestamp
def fetch_latest_inserted_at(connection, logger):
    """Fetches the latest inserted_at value from the destination table."""
    try:
        with connection.cursor() as cursor:
            query = sql.SQL("""
                SELECT COALESCE(MAX(inserted_at), '1970-01-01 00:00:00'::timestamp)
                FROM {table};
            """).format(table=sql.Identifier(DESTINATION_TABLE))
            cursor.execute(query)
            latest_inserted_at = cursor.fetchone()[0]
            logger.info(f"Latest inserted_at: {latest_inserted_at}")
            return latest_inserted_at
    except Exception as e:
        logger.error(f"Error fetching latest inserted_at: {e}")
        raise

# Transform data
def transform_data(batch, connection, logger):
    """Transforms the data for upsertion."""
    transformed_data = []
    try:
        for row in batch:
            inserted_at, ticker, trade_date, close_price, volume = row

            # Calculate 52-week high and low
            one_year_ago = trade_date - relativedelta(years=1)
            with connection.cursor() as cursor:
                cursor.execute('''
                SELECT close_price, trade_date
                FROM raw_algo_seed
                WHERE ticker = %s AND trade_date BETWEEN %s AND %s
                ORDER BY trade_date;
                ''', (ticker, one_year_ago, trade_date))
                historical_data = cursor.fetchall()

            if historical_data:
                w52_high = max(historical_data, key=lambda x: x[0])[0]
                w52_high_date = next(date for price, date in historical_data if price == w52_high)
                w52_low = min(historical_data, key=lambda x: x[0])[0]
                w52_low_date = next(date for price, date in historical_data if price == w52_low)
            else:
                w52_high = w52_low = close_price
                w52_high_date = w52_low_date = trade_date

            is_boh_ok = w52_low_date > w52_high_date and close_price > 1.06 * w52_low
            is_ath_ok = close_price * 1.12 < w52_high

            transformed_data.append((
                datetime.now(), ticker, trade_date, close_price, volume,
                w52_high, w52_high_date, w52_low, w52_low_date,
                is_boh_ok, is_ath_ok
            ))

    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise

    return transformed_data

# Upsert data
def upsert_data(connection, batch, logger):
    """Upserts a batch of data into the destination table."""
    try:
        with connection.cursor() as cursor:
            upsert_query = sql.SQL("""
                INSERT INTO {table} (
                    inserted_at, ticker, trade_date, close_price, volume,
                    w52_high, w52_high_date, w52_low, w52_low_date, is_boh_ok, is_ath_ok
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, trade_date)
                DO UPDATE SET
                    inserted_at = EXCLUDED.inserted_at,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    w52_high = EXCLUDED.w52_high,
                    w52_high_date = EXCLUDED.w52_high_date,
                    w52_low = EXCLUDED.w52_low,
                    w52_low_date = EXCLUDED.w52_low_date,
                    is_boh_ok = EXCLUDED.is_boh_ok,
                    is_ath_ok = EXCLUDED.is_ath_ok
                WHERE
                    {table}.close_price IS DISTINCT FROM EXCLUDED.close_price OR
                    {table}.volume IS DISTINCT FROM EXCLUDED.volume;
            """).format(table=sql.Identifier(DESTINATION_TABLE))
            cursor.executemany(upsert_query, batch)
            connection.commit()
            logger.info(f"Upserted {cursor.rowcount} records.")
    except Exception as e:
        logger.error(f"Error upserting data: {e}")
        raise

# Main function
def main():
    logger = setup_logger("stg_algo_seed_hl")
    logger.info("Starting stg_algo_seed_hl script.")

    try:
        # Load configuration
        db_config, _ = load_config()

        # Connect to the database
        with get_db_connection(db_config) as connection:
            # Create table and indexes
            create_table_and_indexes(connection, logger)

            # Fetch latest inserted_at
            latest_inserted_at = fetch_latest_inserted_at(connection, logger)

            # Fetch and process data in batches
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("""
                    SELECT inserted_at, ticker, trade_date, close_price, volume
                    FROM {source_table}
                    WHERE inserted_at > %s
                    ORDER BY ticker, trade_date, inserted_at;
                """).format(source_table=sql.Identifier(SOURCE_TABLE)), (latest_inserted_at,))
                filtered_data = cursor.fetchall()

            total_batches = len(filtered_data) // BATCH_SIZE + (1 if len(filtered_data) % BATCH_SIZE != 0 else 0)
            logger.info(f"Total records to process: {len(filtered_data)}. Total batches: {total_batches}.")

            for batch_number, batch in enumerate(chunk_list(filtered_data, BATCH_SIZE), start=1):
                logger.info(f"Processing batch {batch_number}/{total_batches}.")
                transformed_batch = transform_data(batch, connection, logger)
                upsert_data(connection, transformed_batch, logger)

            logger.info("Upsert completed successfully.")

    except Exception as e:
        logger.error(f"Error executing script: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
