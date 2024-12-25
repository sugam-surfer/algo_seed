# scripts/raw_algo_seed.py

import sys
from pathlib import Path

# Add parent directory to sys.path to allow module imports
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

from logger import setup_logger
from db_utils import get_db_connection, table_exists, create_table
from config import load_config
from utils import parse_trade_date, chunk_list
import logging
from datetime import datetime
import psycopg2

# Dataflow configuration with individual source tables and preserve_max_inserted_at flag
DATAFLOWS = [
    {
        "source_table": "input_algo_re_seed",
        "target_table": "history_algo_seed",
        "preserve_max_inserted_at": False,
    },
    {
        "source_table": "input_algo_seed",
        "target_table": "raw_algo_seed",
        "preserve_max_inserted_at": False,
    },
    {
     	"source_table": "input_algo_extra_seed",
        "target_table": "raw_algo_seed",
        "preserve_max_inserted_at": True,
    },
    {
        "source_table": "history_algo_seed",
        "target_table": "raw_algo_seed",
        "preserve_max_inserted_at": True,
    },
]

# Batch size
BATCH_SIZE = 5000

def get_max_inserted_at(cursor, table_name):
    """Fetch the max inserted_at timestamp from the target table."""
    query = f"SELECT COALESCE(MAX(inserted_at), '1970-01-01 00:00:00'::timestamp) AS max_inserted_at FROM {table_name}"
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result else datetime.min

def fetch_batch(cursor, source_table, max_inserted_at, offset, batch_size):
    """Fetch a batch of data from the source table."""
    query = f"""
        SELECT inserted_at, ticker, trade_date, close_price, volume
        FROM {source_table}
        WHERE inserted_at > %s
        ORDER BY ticker, trade_date, inserted_at ASC
        LIMIT %s OFFSET %s
    """
    cursor.execute(query, (max_inserted_at, batch_size, offset))
    return cursor.fetchall()

def upsert_batch(cursor, target_table, batch, total_records, offset, logger):
    """Upsert a batch of data into the target table."""
    query = f"""
        INSERT INTO {target_table} (inserted_at, ticker, trade_date, close_price, volume)
        VALUES (CURRENT_TIMESTAMP, %s, %s, %s, %s)
        ON CONFLICT (ticker, trade_date)
        DO UPDATE SET
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            inserted_at = CURRENT_TIMESTAMP
        WHERE {target_table}.close_price IS DISTINCT FROM EXCLUDED.close_price
           OR {target_table}.volume IS DISTINCT FROM EXCLUDED.volume;
    """
    # Clean and prepare batch data
    cleaned_batch = []
    for item in batch:
        try:
            ticker = item[1]
            trade_date = parse_trade_date(item[2], "%Y-%m-%d")  # Enhanced to handle date objects
            close_price = float(item[3]) if item[3] is not None else None
            volume = int(item[4]) if item[4] is not None else None
            cleaned_batch.append((ticker, trade_date, close_price, volume))
        except Exception as e:
            logger.error(f"Error parsing row {item}: {e}")
            continue

    records_changed = 0

    try:
        cursor.executemany(query, cleaned_batch)
        records_changed = cursor.rowcount

        # Calculate processed percentage based on total records processed so far
        processed_percentage = (offset + len(batch)) / total_records * 100 if total_records else 0

        # Log the detailed batch processing info
        logger.info(
            f"Upserted batch {offset // BATCH_SIZE + 1} with {len(cleaned_batch)} records. "
            f"Records upserted = {records_changed}. Processed = {processed_percentage:.2f}%."
        )

    except Exception as e:
        logger.error(f"Error during upsert: {e}")
        raise

    return records_changed

def process_dataflow(connection, source_table, target_table, preserve_max_inserted_at=False, previous_max_inserted_at=None, logger=None):
    """Process a single dataflow from source to target table."""
    logger.info(f"Starting dataflow: {source_table} → {target_table}")

    with connection.cursor() as cursor:
        # Determine max_inserted_at value based on the preserve_max_inserted_at flag
        if preserve_max_inserted_at and previous_max_inserted_at:
            # Use the max_inserted_at passed from the previous dataflow (no recalculation)
            max_inserted_at = previous_max_inserted_at
            logger.info(f"Borrowing max_inserted_at from previous flow: {max_inserted_at}")
        else:
            # Get max_inserted_at from the current target table
            max_inserted_at = get_max_inserted_at(cursor, target_table)
            logger.info(f"Max inserted_at value for {target_table}: {max_inserted_at}")

        # Step 2: Get total records from source table for logging processed percentage
        cursor.execute(f"SELECT COUNT(*) FROM {source_table} WHERE inserted_at > %s", (max_inserted_at,))
        total_records = cursor.fetchone()[0]
        logger.info(f"Total records to process for {source_table}: {total_records}")

        # Log the total batches and batch size
        total_batches = (total_records // BATCH_SIZE) + (1 if total_records % BATCH_SIZE != 0 else 0)
        logger.info(f"Total batches: {total_batches} | Batch size: {BATCH_SIZE}")

        # Step 3: Batch processing
        offset = 0
        total_records_changed = 0
        while True:
            # Fetch a batch of data
            batch = fetch_batch(cursor, source_table, max_inserted_at, offset, BATCH_SIZE)
            if not batch:
                break

            # Step 4: Upsert batch into the target table and track records changed
            records_changed = upsert_batch(cursor, target_table, batch, total_records, offset, logger)
            total_records_changed += records_changed

            # Step 5: Update offset for next batch
            offset += len(batch)

        # Step 6: Final summary log
        logger.info(
            f"Upsert completed. Total batches = {total_batches}, "
            f"Total records upserted = {total_records_changed}, "
            f"Total records processed = {total_records}."
        )

        logger.info(f"Dataflow completed successfully for {source_table} → {target_table}.\n")
        return max_inserted_at  # Return max_inserted_at value for passing to the next dataflow

def main():
    """Main script execution."""
    logger = setup_logger("raw_algo_seed")

    logger.info("\nRunning script raw_algo_seed.py")

    try:
        # Load configurations
        db_config, _ = load_config()  # Assuming load_config returns (db_config, gs_config)
        
        # Establish database connection using 'with' statement
        with get_db_connection(db_config) as connection:
            logger.info("Connected to PostgreSQL successfully.\n")

            # Variable to track max_inserted_at across dataflows
            previous_max_inserted_at = None

            # Process each dataflow
            for dataflow in DATAFLOWS:
                source_table = dataflow["source_table"]
                target_table = dataflow["target_table"]
                preserve_max_inserted_at = dataflow["preserve_max_inserted_at"]

                # Process the dataflow
                if preserve_max_inserted_at and previous_max_inserted_at is not None:
                    previous_max_inserted_at = process_dataflow(
                        connection,
                        source_table,
                        target_table,
                        preserve_max_inserted_at=True,
                        previous_max_inserted_at=previous_max_inserted_at,
                        logger=logger
                    )
                else:
                    previous_max_inserted_at = process_dataflow(
                        connection,
                        source_table,
                        target_table,
                        preserve_max_inserted_at=False,
                        logger=logger
                    )

    except Exception as e:
        logger.error(f"Error in the script execution: {e}")
    finally:
        # No need to manually close the connection; 'with' statement handles it
        logger.info("Script completed successfully.")

if __name__ == "__main__":
    main()
