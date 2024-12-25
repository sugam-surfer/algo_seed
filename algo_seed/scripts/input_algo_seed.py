
# scripts/input_algo_seed.py

import sys
from pathlib import Path

# Add parent directory to sys.path to allow module imports
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

# Import modules
import argparse
import sys
from logger import setup_logger
from db_utils import get_db_connection, table_exists, create_table
from config import load_config
from utils import parse_trade_date, chunk_list
import gspread
from google.oauth2.service_account import Credentials
from psycopg2 import sql
from datetime import datetime

def parse_arguments():
    parser = argparse.ArgumentParser(description="Google Sheets to PostgreSQL Upsert")
    parser.add_argument("sheet_id", help="Google Sheets ID")
    parser.add_argument("sheet_name", help="Name of the sheet within the Google Sheet")
    parser.add_argument("table_name", help="PostgreSQL table name for upsert")
    args = parser.parse_args()
    return args.sheet_id, args.sheet_name, args.table_name

def authenticate_google_sheets(gs_config):
    credentials = Credentials.from_service_account_file(
        gs_config['service_account_file'],
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    return gspread.authorize(credentials)

def get_sheet_data(client, sheet_id, sheet_name):
    sheet = client.open_by_key(sheet_id)
    worksheet = sheet.worksheet(sheet_name)
    return worksheet.get_all_records()

def create_upsert_table(cursor, table_name, logger):
    columns = {
        "inserted_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        "ticker": "TEXT NOT NULL",
        "trade_date": "DATE NOT NULL",
        "close_price": "NUMERIC(10, 2)",
        "volume": "INTEGER",
    }
    unique_constraints = ["ticker", "trade_date"]
    indexes = ["inserted_at", "ticker", "trade_date"]
    create_table(cursor, table_name, columns, unique_constraints, indexes)
    logger.info(f"Table '{table_name}' created or already exists.")

def upsert_data(cursor, data, table_name, logger, batch_size=5000):
    column_mapping = {
        "TICKER": "ticker",
        "DATE1": "trade_date",
        "CLOSEPRICE": "close_price",
        "VOLUME": "volume"
    }

    upsert_query = sql.SQL("""
    INSERT INTO {table} (inserted_at, ticker, trade_date, close_price, volume)
    VALUES (DEFAULT, %s, %s, %s, %s)
    ON CONFLICT (ticker, trade_date)
    DO UPDATE SET
        close_price = EXCLUDED.close_price,
        volume = EXCLUDED.volume,
        inserted_at = CURRENT_TIMESTAMP
    WHERE 
        EXCLUDED.close_price IS DISTINCT FROM {table}.close_price OR
        EXCLUDED.volume IS DISTINCT FROM {table}.volume 
    """).format(table=sql.Identifier(table_name))

    total_records_changed = 0
    total_rows = len(data)
    total_batches = (total_rows + batch_size - 1) // batch_size

    logger.info(f"Total batches: {total_batches} | Batch size: {batch_size}")

    for batch_number, batch in enumerate(chunk_list(data, batch_size), start=1):
        formatted_batch = []

        for row in batch:
            mapped_row = {
                db_col: row.get(sheet_col)
                for sheet_col, db_col in column_mapping.items()
            }

            try:
                ticker = mapped_row["ticker"]
                trade_date = parse_trade_date(mapped_row["trade_date"], "%d-%b-%Y")
                close_price = float(mapped_row["close_price"]) if mapped_row["close_price"] else None
                volume = int(mapped_row["volume"]) if mapped_row["volume"] else None

                if not ticker or not trade_date:
                    continue

                formatted_batch.append((ticker, trade_date, close_price, volume))
            except Exception as e:
                logger.error(f"Error parsing row {row}: {e}")
                continue

        if formatted_batch:
            cursor.executemany(upsert_query, formatted_batch)
            records_changed = cursor.rowcount
            total_records_changed += records_changed

            processed_percentage = (batch_number * batch_size) / total_rows * 100
            logger.info(
                f"Upserted batch {batch_number} with {len(formatted_batch)} records. "
                f"Records upserted = {records_changed}. Processed = {processed_percentage:.2f}%."
            )

    logger.info(
        f"Upsert completed. Total batches = {total_batches}, "
        f"Total records upserted = {total_records_changed}, "
        f"Total records processed = {total_rows}."
    )

def main():
    sheet_id, sheet_name, table_name = parse_arguments()
    logger = setup_logger("input_algo_seed")

    logger.info(f"Running script input_algo_seed.py")

    try:
        db_config, gs_config = load_config()
        client = authenticate_google_sheets(gs_config)
        logger.info("Google Sheets authentication successful.")

        data = get_sheet_data(client, sheet_id, sheet_name)
        logger.info(f"Fetched {len(data)} rows from sheet '{sheet_name}'.")

        with get_db_connection(db_config) as connection:
            with connection.cursor() as cursor:
                if not table_exists(cursor, table_name):
                    create_upsert_table(cursor, table_name, logger)
                else:
                    logger.info(f"Table '{table_name}' already exists.")

                upsert_data(cursor, data, table_name, logger, batch_size=5000)

                connection.commit()

    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

    logger.info("Database connection closed.")

if __name__ == "__main__":
    main()
