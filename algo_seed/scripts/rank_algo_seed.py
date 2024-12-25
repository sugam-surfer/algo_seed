import argparse
import psycopg2
from psycopg2 import sql
import sys
from pathlib import Path

# Dynamically add parent directory of the script to sys.path to include logger.py
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

from logger import setup_logger
from db_utils import get_db_connection

threshold_ratio = 0.9  # Configurable threshold for ticker availability

default_process_days = 60  # Configurable: Number of days to process for an existing table
historical_process_days = 999999  # Configurable: Number of days to process for a new table

def parse_arguments():
    """Parse CLI arguments for the script."""
    parser = argparse.ArgumentParser(description="Process input and output table names dynamically.")
    parser.add_argument(
        "--input_table",
        required=True,
        help="The input table name in the format 'stg_algo_seed_<n_value>'"
    )
    parser.add_argument(
        "--process_latest",
        type=lambda x: x.lower() == 'true',
        default=True,
        help="Whether to process the latest data (default: True). If False, processes historical data."
    )
    args = parser.parse_args()

    # Extract n_value from the input table and derive the output table name
    if not args.input_table.startswith("stg_algo_seed_"):
        raise ValueError("Input table name must follow the pattern 'stg_algo_seed_<n_value>'.")
    
    n_value = args.input_table.split("_")[-1]  # Extract <n_value> from input table
    output_table = f"rank_algo_seed_{n_value}"  # Construct output table name

    return args.input_table, output_table, n_value, args.process_latest


def get_dynamic_columns(n_value):
    """
    Generate dynamic column names based on the n_value.

    Args:
        n_value (str): The n_value extracted from the input table.

    Returns:
        dict: A dictionary of dynamic column names.
    """
    return {
        "avg_day_price": f"avg_{n_value}_day_price",
        "avg_day_volume": f"avg_{n_value}_day_volume",
        "median_day_volume": f"median_{n_value}_day_volume"
    }


def create_dynamic_table(connection, output_table, dynamic_columns, logger):
    """
    Check if the output table exists. If not, create it dynamically with required schema and indexes.
    """
    try:
        cursor = connection.cursor()

        # Check if the table exists
        table_exists_query = sql.SQL("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = %s
            );
        """)
        cursor.execute(table_exists_query, (output_table,))
        table_exists = cursor.fetchone()[0]

        if table_exists:
            logger.info(f"Output table '{output_table}' already exists.")
        else:
            # Define the table schema and indexes
            create_table_query = sql.SQL("""
                CREATE TABLE {table} (
                    ticker VARCHAR(50) NOT NULL,
                    type VARCHAR(50),
                    category VARCHAR(50),
                    sector VARCHAR(50),
                    trade_date DATE NOT NULL,
                    close_price NUMERIC,
                    volume NUMERIC,
                    {avg_price_col} NUMERIC,
                    {avg_volume_col} NUMERIC,
                    {median_volume_col} NUMERIC,
                    is_volume_ok BOOLEAN,
                    is_boh_ok BOOLEAN,
                    is_ath_ok BOOLEAN,
                    is_tradeable BOOLEAN,
                    inserted_at TIMESTAMP NOT NULL,
                    price_gap NUMERIC,
                    w52_high NUMERIC,
                    w52_high_date DATE,
                    w52_low NUMERIC,
                    w52_low_date DATE,
                    rank_all NUMERIC,
                    rank_etf_equity NUMERIC,
                    rank_etf_gold_silver NUMERIC,
                    rank_etf_all NUMERIC,
                    rank_nifty50 NUMERIC,
                    rank_niftynext50 NUMERIC,
                    rank_nifty50_niftynext50 NUMERIC,
                    rank_equity NUMERIC,
                    PRIMARY KEY (ticker, trade_date)
                );
                
                CREATE INDEX IF NOT EXISTS idx_ticker_trade_date ON {table} (ticker, trade_date);
                CREATE INDEX IF NOT EXISTS idx_inserted_at ON {table} (inserted_at);
            """).format(
                table=sql.Identifier(output_table),
                avg_price_col=sql.Identifier(dynamic_columns["avg_day_price"]),
                avg_volume_col=sql.Identifier(dynamic_columns["avg_day_volume"]),
                median_volume_col=sql.Identifier(dynamic_columns["median_day_volume"])
            )

            # Execute the table creation
            cursor.execute(create_table_query)
            connection.commit()
            logger.info(f"Output table '{output_table}' created successfully.")

        cursor.close()
        return table_exists

    except Exception as e:
        logger.error(f"Error during table creation or existence check: {e}")
        connection.rollback()
        sys.exit(1)


def fetch_trade_dates(connection, table_name, process_last_n_days, logger):
    """
    Fetch distinct trade dates to process from the input table.

    Args:
        connection (psycopg2.connection): Active database connection.
        table_name (str): Name of the input table to query.
        process_last_n_days (int): Number of days to fetch trade dates for.
        logger (logging.Logger): Logger instance for logging information and errors.

    Returns:
        list: A list of distinct trade dates to process.
    """
    try:
        cursor = connection.cursor()

        # Fetch distinct trade dates
        query = sql.SQL("""
            SELECT DISTINCT trade_date
            FROM {table}
            WHERE trade_date >= CURRENT_DATE - INTERVAL '{days} days'
            ORDER BY trade_date;
        """).format(
            table=sql.Identifier(table_name),
            days=sql.Literal(process_last_n_days)
        )
        cursor.execute(query)
        trade_dates = [row[0] for row in cursor.fetchall()]

        logger.info(f"Fetched {len(trade_dates)} trade dates to process.")
        return trade_dates

    except Exception as e:
        logger.error(f"Error fetching trade dates: {e}")
        connection.rollback()
        sys.exit(1)


def process_trade_date(connection, input_table, ref_table, hl_table, output_table, dynamic_columns, trade_date, logger):
    """
    Processes data for a single trade date and upserts it into the output table.
    """
    try:
        cursor = connection.cursor()

        # Calculate total tickers up to and including the trade_date
        cursor.execute(sql.SQL("""
            SELECT COUNT(DISTINCT ticker)
            FROM {table}
            WHERE trade_date <= %s
        """).format(table=sql.Identifier(input_table)), (trade_date,))
        total_tickers = cursor.fetchone()[0]

        # Calculate tickers available for the specific trade_date
        cursor.execute(sql.SQL("""
            SELECT COUNT(DISTINCT ticker)
            FROM {table}
            WHERE trade_date = %s
        """).format(table=sql.Identifier(input_table)), (trade_date,))
        available_tickers = cursor.fetchone()[0]

        # Check against the threshold
        if available_tickers < threshold_ratio * total_tickers:
            logger.info(f"Skipping trade_date {trade_date}: Only {available_tickers}/{total_tickers} tickers available "
                        f"({available_tickers / total_tickers:.2%} < {threshold_ratio:.0%}).")
            return

        sys.stdout.write(f"\rProcessing trade_date: {trade_date}... Rows upserted: -")
        sys.stdout.flush()

        query = sql.SQL("""
            INSERT INTO {output_table} (
                ticker,
                type,
                category,
                sector,
                trade_date,
                close_price,
                volume,
                {avg_price_col},
                {avg_volume_col},
                {median_volume_col},
                is_volume_ok,
                is_boh_ok,
                is_ath_ok,
                is_tradeable,
                inserted_at,
                price_gap,
                w52_high,
                w52_high_date,
                w52_low,
                w52_low_date,
                rank_all,
                rank_etf_equity,
                rank_etf_gold_silver,
                rank_etf_all,
                rank_nifty50,
                rank_niftynext50,
                rank_nifty50_niftynext50,
                rank_equity
            )
            SELECT 
                s020.ticker,
                ref.type,
                ref.category,
                ref.sector,
                s020.trade_date,
                s020.close_price,
                s020.volume,
                s020.{avg_price_col},
                s020.{avg_volume_col},
                s020.{median_volume_col},
                s020.is_volume_ok,
                hl.is_boh_ok,
                hl.is_ath_ok,
                s020.is_tradeable,
                CURRENT_TIMESTAMP AS inserted_at,
                (s020.close_price / NULLIF(s020.{avg_price_col}, 0)) - 1 AS price_gap,
                hl.w52_high,
                hl.w52_high_date,
                hl.w52_low,
                hl.w52_low_date,
                RANK() OVER (
                    PARTITION BY s020.trade_date 
                    ORDER BY (s020.close_price / NULLIF(s020.{avg_price_col}, 0)) - 1
                ) AS rank_all,
                RANK() OVER (
                    PARTITION BY s020.trade_date 
                    ORDER BY 
                        CASE 
                            WHEN ref.type = 'ETF' AND ref.category = 'EQUITY' 
                            THEN (s020.close_price / NULLIF(s020.{avg_price_col}, 0)) - 1 
                            ELSE NULL 
                        END
                ) AS rank_etf_equity,
                RANK() OVER (
                    PARTITION BY s020.trade_date 
                    ORDER BY 
                        CASE 
                            WHEN ref.type = 'ETF' AND ref.category IN ('GOLD', 'SILVER') 
                            THEN (s020.close_price / NULLIF(s020.{avg_price_col}, 0)) - 1 
                            ELSE NULL 
                        END
                ) AS rank_etf_gold_silver,
                RANK() OVER (
                    PARTITION BY s020.trade_date 
                    ORDER BY 
                        CASE 
                            WHEN ref.type = 'ETF' 
                            THEN (s020.close_price / NULLIF(s020.{avg_price_col}, 0)) - 1 
                            ELSE NULL 
                        END
                ) AS rank_etf_all,
                RANK() OVER (
                    PARTITION BY s020.trade_date 
                    ORDER BY 
                        CASE 
                            WHEN ref.type = 'NIFTY50' 
                            THEN (s020.close_price / NULLIF(s020.{avg_price_col}, 0)) - 1 
                            ELSE NULL 
                        END
                ) AS rank_nifty50,
                RANK() OVER (
                    PARTITION BY s020.trade_date 
                    ORDER BY 
                        CASE 
                            WHEN ref.type = 'NIFTYNEXT50' 
                            THEN (s020.close_price / NULLIF(s020.{avg_price_col}, 0)) - 1 
                            ELSE NULL 
                        END
                ) AS rank_niftynext50,
                RANK() OVER (
                    PARTITION BY s020.trade_date 
                    ORDER BY 
                        CASE 
                            WHEN ref.type IN ('NIFTY50', 'NIFTYNEXT50') 
                            THEN (s020.close_price / NULLIF(s020.{avg_price_col}, 0)) - 1 
                            ELSE NULL 
                        END
                ) AS rank_nifty50_niftynext50,
                RANK() OVER (
                    PARTITION BY s020.trade_date 
                    ORDER BY 
                        CASE 
                            WHEN (ref.type = 'ETF' AND ref.category = 'EQUITY') OR ref.type IN ('NIFTY50', 'NIFTYNEXT50') 
                            THEN (s020.close_price / NULLIF(s020.{avg_price_col}, 0)) - 1 
                            ELSE NULL 
                        END
                ) AS rank_equity
            FROM {input_table} s020
            LEFT JOIN {ref_table} ref ON s020.ticker = ref.ticker
            LEFT JOIN {hl_table} hl ON s020.ticker = hl.ticker AND s020.trade_date = hl.trade_date
            WHERE s020.trade_date = %s
            ON CONFLICT (ticker, trade_date)
            DO UPDATE SET
                type = EXCLUDED.type,
                category = EXCLUDED.category,
                sector = EXCLUDED.sector,
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume,
                {avg_price_col} = EXCLUDED.{avg_price_col},
                {avg_volume_col} = EXCLUDED.{avg_volume_col},
                {median_volume_col} = EXCLUDED.{median_volume_col},
                is_volume_ok = EXCLUDED.is_volume_ok,
                is_boh_ok = EXCLUDED.is_boh_ok,
                is_ath_ok = EXCLUDED.is_ath_ok,
                is_tradeable = EXCLUDED.is_tradeable,
                inserted_at = CURRENT_TIMESTAMP,
                price_gap = EXCLUDED.price_gap,
                w52_high = EXCLUDED.w52_high,
                w52_high_date = EXCLUDED.w52_high_date,
                w52_low = EXCLUDED.w52_low,
                w52_low_date = EXCLUDED.w52_low_date
            WHERE
                {output_table}.type IS DISTINCT FROM EXCLUDED.type OR
                {output_table}.category IS DISTINCT FROM EXCLUDED.category OR
                {output_table}.sector IS DISTINCT FROM EXCLUDED.sector OR
                {output_table}.close_price IS DISTINCT FROM EXCLUDED.close_price OR
                {output_table}.volume IS DISTINCT FROM EXCLUDED.volume OR
                {output_table}.{avg_price_col} IS DISTINCT FROM EXCLUDED.{avg_price_col} OR
                {output_table}.{avg_volume_col} IS DISTINCT FROM EXCLUDED.{avg_volume_col} OR
                {output_table}.{median_volume_col} IS DISTINCT FROM EXCLUDED.{median_volume_col} OR
                {output_table}.is_volume_ok IS DISTINCT FROM EXCLUDED.is_volume_ok OR
                {output_table}.is_boh_ok IS DISTINCT FROM EXCLUDED.is_boh_ok OR
                {output_table}.is_ath_ok IS DISTINCT FROM EXCLUDED.is_ath_ok OR
                {output_table}.is_tradeable IS DISTINCT FROM EXCLUDED.is_tradeable OR
                {output_table}.price_gap IS DISTINCT FROM EXCLUDED.price_gap OR
                {output_table}.w52_high IS DISTINCT FROM EXCLUDED.w52_high OR
                {output_table}.w52_high_date IS DISTINCT FROM EXCLUDED.w52_high_date OR
                {output_table}.w52_low IS DISTINCT FROM EXCLUDED.w52_low OR
                {output_table}.w52_low_date IS DISTINCT FROM EXCLUDED.w52_low_date;
        """).format(
            input_table=sql.Identifier(input_table),
            ref_table=sql.Identifier(ref_table),
            hl_table=sql.Identifier(hl_table),
            output_table=sql.Identifier(output_table),
            avg_price_col=sql.Identifier(dynamic_columns["avg_day_price"]),
            avg_volume_col=sql.Identifier(dynamic_columns["avg_day_volume"]),
            median_volume_col=sql.Identifier(dynamic_columns["median_day_volume"])
        )

        cursor.execute(query, (trade_date,))
        rows_inserted = cursor.rowcount

        sys.stdout.write(f"\rProcessing trade_date: {trade_date}... Rows upserted: {rows_inserted}\n")
        sys.stdout.flush()

        connection.commit()

    except Exception as e:
        logger.error(f"Error during processing for trade_date {trade_date}: {e}")
        connection.rollback()
        sys.exit(1)


def main():
    """
    Main function to execute the rank_algo_seed process.
    """
    # Step 1: Parse arguments
    input_table, output_table, n_value, process_latest = parse_arguments()
    dynamic_columns = get_dynamic_columns(n_value)

    # Set up logger
    logger = setup_logger("rank_algo_seed")
    logger.info("Running script rank_algo_seed.py")
    logger.info(f"Dynamic columns resolved: {dynamic_columns}")

    # Load database configuration
    try:
        with get_db_connection() as connection:
            logger.info("Connected to PostgreSQL successfully.")

            # Step 2: Check/create output table
            table_exists = create_dynamic_table(connection, output_table, dynamic_columns, logger)

            # Step 3: Fetch trade dates
            if process_latest:
                if table_exists:
                    process_last_n_days = default_process_days
                else:
                    process_last_n_days = historical_process_days
            else:
                process_last_n_days = historical_process_days

            trade_dates = fetch_trade_dates(connection, input_table, process_last_n_days, logger)

            # Step 4: Process data for each trade_date
            for trade_date in trade_dates:
                process_trade_date(connection, input_table, "ref_algo_seed", "stg_algo_seed_hl", output_table, dynamic_columns, trade_date, logger)

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

    logger.info("Script completed successfully.")


if __name__ == "__main__":
    main()
