# scripts/stg_algo_seed.py

import sys
from pathlib import Path
import argparse  # Added missing import

# Add parent directory to sys.path to allow module imports
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

from logger import setup_logger
from db_utils import get_db_connection
from config import load_config
from utils import parse_trade_date, chunk_list
import logging
from datetime import datetime
import psycopg2
from psycopg2 import sql

# Constants
BATCH_SIZE = 5000

def create_dynamic_table(connection, n_value, logger):
    """
    Creates a dynamic staging table and associated indexes based on the provided n_value.

    Args:
        connection (psycopg2.connection): Active database connection.
        n_value (int): The n_value used to name the table and its columns.
        logger (logging.Logger): Logger instance for logging information and errors.

    Returns:
        str: The name of the created or existing table.
    """
    try:
        cursor = connection.cursor()

        # Dynamically create table name and column names based on n_value
        table_suffix = str(n_value).zfill(3)
        table_name = f"stg_algo_seed_{table_suffix}"
        avg_column = f"avg_{table_suffix}_day_price"
        avg_volume_column = f"avg_{table_suffix}_day_volume"
        median_volume_column = f"median_{table_suffix}_day_volume"

        # Define SQL for table and index creation
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table} (
                inserted_at TIMESTAMP WITHOUT TIME ZONE,
                ticker TEXT,
                trade_date DATE,
                close_price DOUBLE PRECISION,
                volume INTEGER,
                {avg_col} DOUBLE PRECISION,
                {avg_vol_col} INTEGER,
                {median_vol_col} INTEGER,
                is_volume_ok BOOLEAN,
                is_tradeable BOOLEAN,
                PRIMARY KEY (ticker, trade_date)
            );
        """).format(
            table=sql.Identifier(table_name),
            avg_col=sql.Identifier(avg_column),
            avg_vol_col=sql.Identifier(avg_volume_column),
            median_vol_col=sql.Identifier(median_volume_column)
        )

        create_indexes_query = sql.SQL("""
            CREATE INDEX IF NOT EXISTS {idx_inserted_at} ON {table} (inserted_at);
            CREATE INDEX IF NOT EXISTS {idx_ticker} ON {table} (ticker);
            CREATE INDEX IF NOT EXISTS {idx_trade_date} ON {table} (trade_date);
        """).format(
            idx_inserted_at=sql.Identifier(f"idx_{table_name}_inserted_at"),
            idx_ticker=sql.Identifier(f"idx_{table_name}_ticker"),
            idx_trade_date=sql.Identifier(f"idx_{table_name}_trade_date"),
            table=sql.Identifier(table_name)
        )

        # Execute table creation
        cursor.execute(create_table_query)
        connection.commit()
        logger.info(f"Table '{table_name}' created or already exists.")

        # Execute index creation
        cursor.execute(create_indexes_query)
        connection.commit()
        logger.info(f"Indexes for table '{table_name}' created or already exist.")

        cursor.close()
        return table_name

    except Exception as e:
        logger.error(f"Error creating table or indexes: {e}")
        connection.close()
        sys.exit(1)

def get_latest_inserted_at(connection, table_name, logger):
    """
    Retrieves the latest inserted_at timestamp from the specified table.

    Args:
        connection (psycopg2.connection): Active database connection.
        table_name (str): Name of the table to query.
        logger (logging.Logger): Logger instance for logging information and errors.

    Returns:
        datetime: The latest inserted_at timestamp.
    """
    try:
        cursor = connection.cursor()
        query = sql.SQL("""
            SELECT COALESCE(MAX(inserted_at), '1970-01-01 00:00:00'::timestamp)
            FROM {table};
        """).format(table=sql.Identifier(table_name))
        cursor.execute(query)
        result = cursor.fetchone()[0]
        logger.info(f"Latest inserted_at in '{table_name}': {result}")
        cursor.close()
        return result
    except Exception as e:
        logger.error(f"Error fetching latest inserted_at: {e}")
        connection.close()
        sys.exit(1)

def execute_batch_upsert(connection, table_name, batch, logger):
    """
    Performs an upsert operation for a batch of records into the staging table.

    Args:
        connection (psycopg2.connection): Active database connection.
        table_name (str): Name of the staging table.
        batch (list of tuples): Batch of records to upsert.
        logger (logging.Logger): Logger instance for logging information and errors.

    Returns:
        int: Number of records upserted.
    """
    try:
        cursor = connection.cursor()

        # Extract table suffix to match column naming
        table_suffix = table_name.split('_')[-1]

        # Define the upsert query
        upsert_query = sql.SQL("""
            INSERT INTO {table} (
                inserted_at,
                ticker,
                trade_date,
                close_price,
                volume,
                {avg_col},
                {avg_vol_col},
                {median_vol_col},
                is_volume_ok,
                is_tradeable
            )
            VALUES (
                CURRENT_TIMESTAMP,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s
            )
            ON CONFLICT (ticker, trade_date)
            DO UPDATE SET
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume,
                {avg_col} = EXCLUDED.{avg_col},
                {avg_vol_col} = EXCLUDED.{avg_vol_col},
                {median_vol_col} = EXCLUDED.{median_vol_col},
                is_volume_ok = EXCLUDED.is_volume_ok,
                is_tradeable = EXCLUDED.is_tradeable,
                inserted_at = CURRENT_TIMESTAMP
            WHERE
                {table}.close_price IS DISTINCT FROM EXCLUDED.close_price
                OR {table}.volume IS DISTINCT FROM EXCLUDED.volume
                OR {table}.{avg_col} IS DISTINCT FROM EXCLUDED.{avg_col}
                OR {table}.{avg_vol_col} IS DISTINCT FROM EXCLUDED.{avg_vol_col}
                OR {table}.{median_vol_col} IS DISTINCT FROM EXCLUDED.{median_vol_col}
                OR {table}.is_volume_ok IS DISTINCT FROM EXCLUDED.is_volume_ok
                OR {table}.is_tradeable IS DISTINCT FROM EXCLUDED.is_tradeable;
        """).format(
            table=sql.Identifier(table_name),
            avg_col=sql.Identifier(f"avg_{table_suffix}_day_price"),
            avg_vol_col=sql.Identifier(f"avg_{table_suffix}_day_volume"),
            median_vol_col=sql.Identifier(f"median_{table_suffix}_day_volume")
        )

        # Execute the upsert
        cursor.executemany(upsert_query, batch)
        records_upserted = cursor.rowcount
        connection.commit()
        cursor.close()
        return records_upserted

    except Exception as e:
        logger.error(f"Error during batch upsert: {e}")
        connection.rollback()
        cursor.close()
        sys.exit(1)

def process_dataflows(connection, table_name, latest_inserted_at, logger):
    """
    Processes dataflows by fetching records from raw_algo_seed and upserting them into the staging table.

    Args:
        connection (psycopg2.connection): Active database connection.
        table_name (str): Name of the staging table.
        latest_inserted_at (datetime): The timestamp to filter new records.
        logger (logging.Logger): Logger instance for logging information and errors.
    """
    try:
        cursor = connection.cursor()
        # Get total records to process
        cursor.execute(sql.SQL("""
            SELECT COUNT(*)
            FROM raw_algo_seed
            WHERE inserted_at > %s;
        """), (latest_inserted_at,))
        total_records = cursor.fetchone()[0]
        cursor.close()

        logger.info(f"Total records to process: {total_records}")
        total_batches = (total_records // BATCH_SIZE) + (1 if total_records % BATCH_SIZE != 0 else 0)
        logger.info(f"Total batches: {total_batches} | Batch size: {BATCH_SIZE}")

        offset = 0
        records_upserted_total = 0

        while offset < total_records:
            cursor = connection.cursor()
            # Fetch a batch of records
            cursor.execute(sql.SQL("""
                SELECT inserted_at, ticker, trade_date, close_price, volume
                FROM raw_algo_seed
                WHERE inserted_at > %s
                ORDER BY ticker, trade_date, inserted_at
                LIMIT %s OFFSET %s;
            """), (latest_inserted_at, BATCH_SIZE, offset))
            batch_records = cursor.fetchall()
            cursor.close()

            if not batch_records:
                break

            # Prepare batch data
            prepared_batch = []
            for record in batch_records:
                try:
                    inserted_at, ticker, trade_date, close_price, volume = record
                    trade_date_parsed = parse_trade_date(trade_date, "%Y-%m-%d")
                    
                    # Placeholder calculations for avg_day_price, avg_day_volume, median_day_volume
                    # These should be replaced with actual logic as per business requirements
                    avg_day_price = None  # Replace with actual average calculation
                    avg_day_volume = None  # Replace with actual average calculation
                    median_day_volume = None  # Replace with actual median calculation
                    
                    # Placeholder logic for is_volume_ok and is_tradeable
                    is_volume_ok = False  # Replace with actual condition
                    is_tradeable = False  # Replace with actual condition

                    prepared_batch.append((
                        ticker,
                        trade_date_parsed,
                        close_price,
                        volume,
                        avg_day_price,
                        avg_day_volume,
                        median_day_volume,
                        is_volume_ok,
                        is_tradeable
                    ))
                except Exception as e:
                    logger.error(f"Error parsing record {record}: {e}")
                    continue

            # Execute batch upsert
            records_upserted = execute_batch_upsert(connection, table_name, prepared_batch, logger)
            records_upserted_total += records_upserted
            offset += BATCH_SIZE

            # Log progress
            processed_percentage = (offset / total_records) * 100 if total_records else 0
            logger.info(f"Upserted batch {offset // BATCH_SIZE} with {len(prepared_batch)} records. "
                        f"Records upserted = {records_upserted}. Processed = {processed_percentage:.2f}%.")
        
        logger.info(f"Total records upserted: {records_upserted_total}")

    except Exception as e:
        logger.error(f"Error processing dataflows: {e}")
        connection.close()
        sys.exit(1)

def main(n_value):
    """
    Main function to execute the staging process.

    Args:
        n_value (int): The n_value used to determine the staging table and its computations.
    """
    logger = setup_logger("stg_algo_seed")

    logger.info(f"\nRunning script stg_algo_seed.py with n_value={n_value}")

    # Load configurations
    db_config, _ = load_config()  # Assuming load_config returns (db_config, gs_config)

    # Establish database connection using context manager
    try:
        with get_db_connection(db_config) as connection:
            logger.info("Connected to PostgreSQL successfully.")

            # Create dynamic table and indexes
            table_name = create_dynamic_table(connection, n_value, logger)

            # Get the latest inserted_at timestamp
            latest_inserted_at = get_latest_inserted_at(connection, table_name, logger)

            # Process dataflows
            process_dataflows(connection, table_name, latest_inserted_at, logger)

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

    logger.info("Staging process completed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Staging script for algo_seed data.")
    parser.add_argument("n_value", type=int, help="The n_value for the staging table.")
    args = parser.parse_args()

    main(args.n_value)
