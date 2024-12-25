import sys
import argparse
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime, date, timedelta

########################################
# Configuration and Defaults
########################################

DB_CONFIG = {
    "host": "postgresqldb-1.cpks8c6m4q9e.ap-south-1.rds.amazonaws.com",
    "port": 5432,
    "dbname": "postgres",
    "user": "sugamkuchhal",
    "password": "hn9t6PTPS",
}

DEFAULT_IS_INCREMENTAL = True
DEFAULT_START_DATE = "2024-11-01"
DEFAULT_END_DATE = date.today().strftime("%Y-%m-%d")
DEFAULT_RANK_COLUMN_SUFFIX = "_EQUITY"
DEFAULT_TOP_N_RANKED_BUY = 5
DEFAULT_TRADES_PER_DAY = 2
DEFAULT_BASE_TRADE_AMOUNT = 50000

RANK_TABLE = "rank_algo_seed_020"
TEMP_TABLE = "temp_rank_data_temp"

LEDGER_TABLE = "prod_algo_seed_ledger"
CREDIT_CANDIDATE_TABLE = "prod_algo_seed_credit_candidate"
CREDIT_TRADE_TABLE = "prod_algo_seed_credit_trade"
DEBIT_TRADE_TABLE = "prod_algo_seed_debit_trade"
CREDIT_TRADE_ADD_TABLE = "prod_algo_seed_credit_trade_add"
DEBIT_TRADE_ADD_TABLE = "prod_algo_seed_debit_trade_add"
PORTFOLIO_BALANCE_TABLE = "op_algo_seed_portfolio_balance"
CAP_MAN_TABLE = "op_algo_seed_cap_man"


########################################
# Logging Setup
########################################
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

########################################
# Argument Parsing
########################################

def parse_arguments():
    parser = argparse.ArgumentParser(description="Process trades.")
    parser.add_argument("--is_incremental", type=str, default=str(DEFAULT_IS_INCREMENTAL),
                        help="Whether to run incrementally (true/false)")
    parser.add_argument("--start_date", type=str, default=DEFAULT_START_DATE,
                        help="Start date in YYYY-MM-DD")
    parser.add_argument("--end_date", type=str, default=DEFAULT_END_DATE,
                        help="End date in YYYY-MM-DD")
    parser.add_argument("--rank_column_suffix", type=str, default=DEFAULT_RANK_COLUMN_SUFFIX,
                        help="Rank column suffix")
    parser.add_argument("--top_n_ranked_buy", type=int, default=DEFAULT_TOP_N_RANKED_BUY,
                        help="Top N ranked to buy")
    parser.add_argument("--TRADES_PER_DAY", type=int, default=DEFAULT_TRADES_PER_DAY,
                        help="Number of trades per day")
    parser.add_argument("--Base_Trade_Amount", type=int, default=DEFAULT_BASE_TRADE_AMOUNT,
                        help="Base trade amount")

    args = parser.parse_args()

    # Convert is_incremental to boolean
    is_incremental = args.is_incremental.lower() == "true"

    # Validate dates
    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    except ValueError:
        logging.error("Invalid date format. Please use YYYY-MM-DD.")
        sys.exit(1)

    if end_date < start_date:
        logging.error("End date cannot be earlier than start date.")
        sys.exit(1)

    return (is_incremental, start_date, end_date, args.rank_column_suffix,
            args.top_n_ranked_buy, args.TRADES_PER_DAY, args.Base_Trade_Amount)

########################################
# Database Utilities
########################################

def get_connection():
    conn = psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        dbname=DB_CONFIG['dbname'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )
    return conn

def table_exists(conn, table_name):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public'
                  AND table_name = %s
            );
        """, (table_name,))
        return cur.fetchone()[0]

def create_tables_if_not_exists(conn):
    # We’ll encapsulate each CREATE TABLE in a function and call them here
    # Also handle indexes
    create_statements = [
        # op_algo_seed_cap_man
        (CAP_MAN_TABLE, f"""
CREATE TABLE IF NOT EXISTS {CAP_MAN_TABLE} (
	trade_level integer,
	split_01 text,
	split_02 text,
	split_03 text,
	split_04 text,
	split_05 text,
	split_06 text,
	split_07 text,
	split_08 text,
	split_09 text,
	split_10 text,
	split_11 text,
	split_12 text,
	split_13 text,
	split_14 text,
	split_15 text,
	split_16 text,
	split_17 text,
	split_18 text,
	split_19 text,
	split_20 text
);"""),

        # op_algo_seed_portfolio_balance
        (PORTFOLIO_BALANCE_TABLE, f"""
CREATE TABLE IF NOT EXISTS {PORTFOLIO_BALANCE_TABLE} (
	credit_trade_id text,
	ticker text,
	credit_trade_date date,
	credit_close_price numeric(10,2),
	debit_trade_date date,
	debit_close_price numeric(10,2),
	balance_date date,
	balance_close_price numeric(10,2),
	upside numeric(10,6),
	balance_amount numeric(10,2),
	ticker_delta integer,
	amount_delta numeric(10,2)
);
CREATE INDEX IF NOT EXISTS idx_ticker_op_algo_seed_portfolio_balance ON {PORTFOLIO_BALANCE_TABLE} (ticker);
CREATE INDEX IF NOT EXISTS idx_balance_date_op_algo_seed_portfolio_balance ON {PORTFOLIO_BALANCE_TABLE} (balance_date);
"""),

        # prod_algo_seed_credit_candidate
        (CREDIT_CANDIDATE_TABLE, f"""
CREATE TABLE IF NOT EXISTS {CREDIT_CANDIDATE_TABLE} (
	credit_trade_id text,
	credit_trade_date date,
	debit_trade_id text,
	debit_trade_date date,
	ticker text,
	type text,
	category text,
	sector text,
	is_boh_ok text,
	is_ath_ok text,
	credit_close_price numeric(10,2),
	process_used text,
	process_rank integer,
	rank_used integer,
	trade_level integer,
	trade_split integer,
	base_amount numeric(10,2),
	trade_units integer,
	trade_amount numeric(10,2),
	trade_close_price numeric(10,2),
	trade_process_used text,
	charges numeric(10,2),
	reco text
);
CREATE INDEX IF NOT EXISTS idx_ticker_prod_algo_seed_credit_candidate ON {CREDIT_CANDIDATE_TABLE} (ticker);
CREATE INDEX IF NOT EXISTS idx_credit_trade_id_prod_algo_seed_credit_candidate ON {CREDIT_CANDIDATE_TABLE} (credit_trade_id);
CREATE INDEX IF NOT EXISTS idx_credit_trade_date_prod_algo_seed_credit_candidate ON {CREDIT_CANDIDATE_TABLE} (credit_trade_date);
CREATE INDEX IF NOT EXISTS idx_debit_trade_id_prod_algo_seed_credit_candidate ON {CREDIT_CANDIDATE_TABLE} (debit_trade_id);
CREATE INDEX IF NOT EXISTS idx_debit_trade_date_prod_algo_seed_credit_candidate ON {CREDIT_CANDIDATE_TABLE} (debit_trade_date);
"""),

        # prod_algo_seed_credit_trade
        (CREDIT_TRADE_TABLE, f"""
CREATE TABLE IF NOT EXISTS {CREDIT_TRADE_TABLE} (
	credit_trade_id text,
	credit_trade_date date,
	debit_trade_id text,
	debit_trade_date date,
	ticker text,
	type text,
	category text,
	sector text,
	is_boh_ok text,
	is_ath_ok text,
	credit_close_price numeric(10,2),
	process_used text,
	process_rank integer,
	rank_used integer,
	trade_level integer,
	trade_split integer,
	base_amount numeric(10,2),
	trade_units integer,
	trade_amount numeric(10,2),
	trade_close_price numeric(10,2),
	trade_process_used text,
	charges numeric(10,2)
);
CREATE INDEX IF NOT EXISTS idx_ticker_prod_algo_seed_credit_trade ON {CREDIT_TRADE_TABLE} (ticker);
CREATE INDEX IF NOT EXISTS idx_credit_trade_id_prod_algo_seed_credit_trade ON {CREDIT_TRADE_TABLE} (credit_trade_id);
CREATE INDEX IF NOT EXISTS idx_credit_trade_date_prod_algo_seed_credit_trade ON {CREDIT_TRADE_TABLE} (credit_trade_date);
CREATE INDEX IF NOT EXISTS idx_debit_trade_id_prod_algo_seed_credit_trade ON {CREDIT_TRADE_TABLE} (debit_trade_id);
CREATE INDEX IF NOT EXISTS idx_debit_trade_date_prod_algo_seed_credit_trade ON {CREDIT_TRADE_TABLE} (debit_trade_date);
"""),

        # prod_algo_seed_credit_trade_add
        (CREDIT_TRADE_ADD_TABLE, f"""
CREATE TABLE IF NOT EXISTS {CREDIT_TRADE_ADD_TABLE} (
	credit_trade_id text,
	credit_trade_date date,
	debit_trade_id text,
	debit_trade_date date,
	ticker text,
	type text,
	category text,
	sector text,
	is_boh_ok text,
	is_ath_ok text,
	credit_close_price numeric(10,2),
	process_used text,
	process_rank integer,
	rank_used integer,
	trade_level integer,
	trade_split integer,
	base_amount numeric(10,2),
	trade_units integer,
	trade_amount numeric(10,2),
	trade_close_price numeric(10,2),
	trade_process_used text,
	charges numeric(10,2),
	scan_date date,
	scan_close_price numeric(10,2)
);
CREATE INDEX IF NOT EXISTS idx_ticker_prod_algo_seed_credit_trade_add ON {CREDIT_TRADE_ADD_TABLE} (ticker);
CREATE INDEX IF NOT EXISTS idx_credit_trade_id_prod_algo_seed_credit_trade_add ON {CREDIT_TRADE_ADD_TABLE} (credit_trade_id);
CREATE INDEX IF NOT EXISTS idx_credit_trade_date_prod_algo_seed_credit_trade_add ON {CREDIT_TRADE_ADD_TABLE} (credit_trade_date);
CREATE INDEX IF NOT EXISTS idx_debit_trade_id_prod_algo_seed_credit_trade_add ON {CREDIT_TRADE_ADD_TABLE} (debit_trade_id);
CREATE INDEX IF NOT EXISTS idx_debit_trade_date_prod_algo_seed_credit_trade_add ON {CREDIT_TRADE_ADD_TABLE} (debit_trade_date);
"""),

        # prod_algo_seed_debit_trade
        (DEBIT_TRADE_TABLE, f"""
CREATE TABLE IF NOT EXISTS {DEBIT_TRADE_TABLE} (
	debit_trade_id text,
	credit_trade_id text,
	ticker text,
	type text,
	category text,
	sector text,
	is_boh_ok text,
	is_ath_ok text,
	credit_trade_date date,
	credit_close_price numeric(10,2),
	debit_trade_date date,
	debit_close_price numeric(10,2),
	closure_days integer,
	trade_amount numeric(10,2),
	upside numeric(10,6),
	charges numeric(10,2),
	taxes numeric(10,2),
	open_trades_limit integer,
	closure_days_limit integer
);
CREATE INDEX IF NOT EXISTS idx_ticker_prod_algo_seed_debit_trade ON {DEBIT_TRADE_TABLE} (ticker);
CREATE INDEX IF NOT EXISTS idx_credit_trade_id_prod_algo_seed_debit_trade ON {DEBIT_TRADE_TABLE} (credit_trade_id);
CREATE INDEX IF NOT EXISTS idx_credit_trade_date_prod_algo_seed_debit_trade ON {DEBIT_TRADE_TABLE} (credit_trade_date);
CREATE INDEX IF NOT EXISTS idx_debit_trade_id_prod_algo_seed_debit_trade ON {DEBIT_TRADE_TABLE} (debit_trade_id);
CREATE INDEX IF NOT EXISTS idx_debit_trade_date_prod_algo_seed_debit_trade ON {DEBIT_TRADE_TABLE} (debit_trade_date);
"""),

        # prod_algo_seed_debit_trade_add
        (DEBIT_TRADE_ADD_TABLE, f"""
CREATE TABLE IF NOT EXISTS {DEBIT_TRADE_ADD_TABLE} (
	debit_trade_id text,
	credit_trade_id text,
	ticker text,
	type text,
	category text,
	sector text,
	is_boh_ok text,
	is_ath_ok text,
	credit_trade_date date,
	credit_close_price numeric(10,2),
	debit_trade_date date,
	debit_close_price numeric(10,2),
	closure_days integer,
	trade_amount numeric(10,2),
	upside numeric(10,6),
	charges numeric(10,2),
	taxes numeric(10,2),
	open_trades_limit integer,
	closure_days_limit integer,
	scan_date date,
	scan_close_price numeric(10,2)
);
CREATE INDEX IF NOT EXISTS idx_ticker_prod_algo_seed_debit_trade_add ON {DEBIT_TRADE_ADD_TABLE} (ticker);
CREATE INDEX IF NOT EXISTS idx_credit_trade_id_prod_algo_seed_debit_trade_add ON {DEBIT_TRADE_ADD_TABLE} (credit_trade_id);
CREATE INDEX IF NOT EXISTS idx_credit_trade_date_prod_algo_seed_debit_trade_add ON {DEBIT_TRADE_ADD_TABLE} (credit_trade_date);
CREATE INDEX IF NOT EXISTS idx_debit_trade_id_prod_algo_seed_debit_trade_add ON {DEBIT_TRADE_ADD_TABLE} (debit_trade_id);
CREATE INDEX IF NOT EXISTS idx_debit_trade_date_prod_algo_seed_debit_trade_add ON {DEBIT_TRADE_ADD_TABLE} (debit_trade_date);
"""),

        # prod_algo_seed_ledger
        (LEDGER_TABLE, f"""
CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} (
	txn_id text,
	txn_date date,
	txn_type text,
	amount numeric(10,6),
	charges_taxes numeric(10,6)
);
""")
    ]

    for table_name, stmt in create_statements:
        if table_exists(conn, table_name):
            logging.info(f"Table {table_name} already exists. Skipping creation.")
        else:
            logging.info(f"Creating table: {table_name}")
            with conn.cursor() as cur:
                for part in stmt.split(";"):
                    p = part.strip()
                    if p:
                        cur.execute(p + ";")
            conn.commit()

########################################
# Business Logic Functions
########################################

def truncate_tables_initial(conn, is_incremental):
    # If non-incremental run, truncate main tables
    if not is_incremental:
        tables_to_truncate = [LEDGER_TABLE, CREDIT_CANDIDATE_TABLE, CREDIT_TRADE_TABLE, DEBIT_TRADE_TABLE]
        for t in tables_to_truncate:
            logging.info(f"Truncating table {t}")
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {t};")
        conn.commit()

    # Always truncate these
    always_truncate = [CREDIT_TRADE_ADD_TABLE, DEBIT_TRADE_ADD_TABLE, PORTFOLIO_BALANCE_TABLE, CAP_MAN_TABLE]
    for t in always_truncate:
        logging.info(f"Truncating table {t}")
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {t};")
    conn.commit()

def get_incremental_dates(conn, is_incremental, start_date, end_date):
    # If incremental, adjust start/end based on db data
    if is_incremental:
        with conn.cursor() as cur:
            # max_trade_date from credit/debit tables
            cur.execute(f"""
            SELECT MAX(x) AS max_trade_date FROM (
               SELECT MAX(credit_trade_date) as x FROM {CREDIT_TRADE_TABLE}
               UNION ALL
               SELECT MAX(debit_trade_date) FROM {DEBIT_TRADE_TABLE}
            ) AS combined;
            """)
            res = cur.fetchone()
            max_trade_date = res[0]

        if max_trade_date is not None:
            start_date = max_trade_date + timedelta(days=1)

        # ensure end_date does not exceed rank table max
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT MAX(trade_date) FROM {RANK_TABLE}
                WHERE trade_date <= %s
            """, (end_date,))
            res = cur.fetchone()
            max_rank_date = res[0]

        if max_rank_date is None:
            # No data to process
            return None, None

        if max_trade_date is not None and max_rank_date < max_trade_date:
            logging.error("No new data to process. Latest credit date is same as rank table date.")
            return None, None

        if end_date > max_rank_date:
            end_date = max_rank_date

        if end_date < start_date:
            logging.error("The end date cannot be earlier than the start date.")
            return None, None

    return start_date, end_date


def create_temp_table(conn, start_date, end_date, rank_column_suffix):
    rank_column = 'rank' + rank_column_suffix
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {TEMP_TABLE};")
        cur.execute(f"""
            CREATE TEMP TABLE {TEMP_TABLE} AS 
            SELECT 
                ticker,
                type,
                category,
                sector,
                is_boh_ok,
                is_ath_ok,
                trade_date,
                closeprice,
                {rank_column} as rank_used
            FROM {RANK_TABLE}
            WHERE trade_date BETWEEN %s AND %s;
        """, (start_date, end_date))
    conn.commit()

def insert_initial_ledger_entry(conn, start_date):
    initialCapital = 2000000
    with conn.cursor() as cur:
        txn_id = f"{start_date.isoformat()} | INITIAL"
        cur.execute(f"""
            INSERT INTO {LEDGER_TABLE}(txn_id, txn_date, txn_type, amount, charges_taxes)
            VALUES (%s, %s, %s, %s, %s)
        """, (txn_id, start_date, 'INITIAL', initialCapital, 0))
    conn.commit()




def process_days(conn, start_date, end_date,
                 top_n_ranked_buy, TRADES_PER_DAY, Base_Trade_Amount, rank_column_suffix, is_incremental):
    """
    Process daily trading logic from start_date to end_date. Adapts the original Snowflake procedure
    to PostgreSQL using psycopg2.

    :param conn: psycopg2 connection object
    :param start_date: Date object for the start date
    :param end_date: Date object for the end date
    :param top_n_ranked_buy: integer specifying how many top-ranked tickers to buy
    :param TRADES_PER_DAY: integer specifying how many trades to insert per day
    :param Base_Trade_Amount: integer representing the base trade amount
    :param rank_column_suffix: string suffix to build the rank column name (e.g., "_EQUITY")
    :param is_incremental: boolean indicating if we are running incrementally
    """

    # Thresholds and constants (mirroring original logic)
    initialCapital = 2000000
    monthlySIP = 200000
    stepupSIP = 0.10
    baseTradeAmount = Base_Trade_Amount
    tradeSplits = 20
    openTradesLimit = 1000
    tradeIncrementRate = 0.040
    debitThresholdETF = 0.050
    debitThresholdNIFTY50 = 0.060
    debitThresholdNIFTYNEXT50 = 0.070
    minDebitThreshold = 0.030

    avgDownThreshold1 = 0.95
    avgDownThreshold2 = 0.90
    avgDownThreshold3 = 0.85

    avgDownRefactor = -0.005

    closureDaysRange = 60
    maxClosureDaysLimit = 3
    closureDaysRefactor = -0.005

    openTradesRange = 20
    maxOpenTradesLimit = 4
    openTradesRefactor = -0.005

    variableBuyCost = 0.0011872
    fixedBuyCost = 23.6
    variableSellCost = 0.0010372
    fixedSellCost = 45.135
    STCG_NON_EQUITY = 0.35
    STCG_EQUITY = 0.2
    LTCG = 0.125

    additionalDays = 15

    rank_column = 'rank' + rank_column_suffix

    # Tables (assuming these are defined in the global scope or passed in as well)
    # If you have them as global constants, ensure they match.
    from datetime import timedelta
    import logging

    LEDGER_TABLE = "prod_algo_seed_ledger"
    CREDIT_CANDIDATE_TABLE = "prod_algo_seed_credit_candidate"
    CREDIT_TRADE_TABLE = "prod_algo_seed_credit_trade"
    DEBIT_TRADE_TABLE = "prod_algo_seed_debit_trade"
    CREDIT_TRADE_ADD_TABLE = "prod_algo_seed_credit_trade_add"
    DEBIT_TRADE_ADD_TABLE = "prod_algo_seed_debit_trade_add"
    PORTFOLIO_BALANCE_TABLE = "op_algo_seed_portfolio_balance"
    CAP_MAN_TABLE = "op_algo_seed_cap_man"
    TEMP_TABLE = "temp_rank_data_temp"  # Or whatever you named your temp table

    tickerSIP = 0  # SIP counter

    current_date = start_date
    while current_date <= end_date:
        formattedWorkDate = current_date.isoformat()
        work_day_str = formattedWorkDate.split("-")[2]

        # Insert SIP into ledger table if it's the 1st of the month and not the start date
        if work_day_str == '01' and current_date != start_date:
            tickerSIP += 1
            sip_amount = monthlySIP * ((1 + stepupSIP) ** (tickerSIP // 12))
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {LEDGER_TABLE} (txn_id, txn_date, txn_type, amount, charges_taxes)
                    SELECT CONCAT(%s, ' | SIP - ', LPAD(%s::text, 3, '0')),
                           %s,
                           'SIP',
                           %s,
                           0
                    FROM {TEMP_TABLE}
                    WHERE %s = '1'
                      AND %s <> %s
                    LIMIT 1;
                """, (
                    formattedWorkDate,
                    tickerSIP,
                    current_date,
                    sip_amount,
                    work_day_str,
                    start_date.isoformat(),
                    formattedWorkDate
                ))
            conn.commit()

        # Insert into DEBIT_TRADE_TABLE
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {DEBIT_TRADE_TABLE}
                WITH 
                temp_table_day_before_work_date AS (
                    SELECT
                        ticker,
                        trade_date,
                        closeprice
                    FROM {TEMP_TABLE}
                    WHERE trade_date = %s::date - INTERVAL '1 day'
                ),
                
                credits_before_work_date_without_debits AS (
                    SELECT 
                        credit_trade_id,
                        credit_trade_date,
                        ticker,
                        type,
                        category,
                        sector,
                        is_boh_ok,
                        is_ath_ok,
                        credit_close_price AS credit_closeprice,
                        LEFT(process_used,1) AS left_1_process_used,
                        trade_amount,
                        CASE 
                            WHEN type = 'ETF' THEN {debitThresholdETF}
                            WHEN type = 'NIFTY50' THEN {debitThresholdNIFTY50}
                            WHEN type = 'NIFTYNEXT50' THEN {debitThresholdNIFTYNEXT50}
                        END as debit_thr
                    FROM {CREDIT_TRADE_TABLE}
                    WHERE debit_trade_id IS NULL
                      AND credit_trade_date < %s::date
                ),

                final_selection AS (
                SELECT 
                    CONCAT(c.credit_trade_id, ' | ', r.trade_date) AS debit_trade_id, 
                    c.credit_trade_id,
                    c.ticker,
                    c.type,
                    c.category,
                    c.sector,
                    c.is_boh_ok,
                    c.is_ath_ok,
                    c.credit_trade_date,
                    c.credit_closeprice,
                    r.trade_date AS debit_trade_date,
                    r.closeprice AS debit_closeprice,
                    EXTRACT(DAY FROM (r.trade_date - c.credit_trade_date)) AS closure_days,
                    c.trade_amount,
                    (r.closeprice - c.credit_closeprice) / c.credit_closeprice AS upside,
                    ({fixedSellCost} 
                     + ({variableSellCost} * (c.trade_amount * 
                        (1 + ((r.closeprice - c.credit_closeprice) / c.credit_closeprice))))) as charges,
                    (
                      ( c.trade_amount * ((r.closeprice - c.credit_closeprice) / c.credit_closeprice) )
                      * (CASE WHEN EXTRACT(DAY FROM (r.trade_date - c.credit_trade_date)) > 366 
                              THEN {LTCG}
                              ELSE 
                                  CASE WHEN c.category = 'EQUITY' THEN {STCG_EQUITY} 
                                       ELSE {STCG_NON_EQUITY} 
                                  END 
                         END)
                    ) as taxes,
                    
                    FLOOR(EXTRACT(DAY FROM (r.trade_date - c.credit_trade_date))/{closureDaysRange}) as finalClosureDaysLimit,
                    (SELECT count(*) FROM credits_before_work_date_without_debits) as open_trades_count,
                    
                    (
                      CASE 
                           WHEN type = 'ETF' THEN {debitThresholdETF}
                           WHEN type = 'NIFTY50' THEN {debitThresholdNIFTY50}
                           WHEN type = 'NIFTYNEXT50' THEN {debitThresholdNIFTYNEXT50}
                      END
                      + (
                          FLOOR(EXTRACT(DAY FROM (r.trade_date - c.credit_trade_date))/{closureDaysRange})
                          * {closureDaysRefactor}
                        )
                      + (
                          c.left_1_process_used::int * {avgDownRefactor}
                        )
                      + (
                          (FLOOR(open_trades_count/{openTradesRange}) - 1)
                           * {openTradesRefactor}
                        )
                    ) AS calc_threshold

                FROM credits_before_work_date_without_debits c
                JOIN temp_table_day_before_work_date r ON c.ticker = r.ticker
                )

                SELECT 
                    debit_trade_id,
                    credit_trade_id,
                    ticker,
                    type,
                    category,
                    sector,
                    is_boh_ok,
                    is_ath_ok,
                    credit_trade_date,
                    credit_closeprice,
                    debit_trade_date,
                    debit_closeprice,
                    closure_days::int,
                    trade_amount,
                    upside,
                    charges,
                    taxes,
                    NULL::int as open_trades_limit,
                    NULL::int as closure_days_limit
                FROM final_selection
                WHERE ( (debit_closeprice - credit_closeprice)/credit_closeprice ) >=
                      GREATEST({minDebitThreshold}, calc_threshold);
            """, (formattedWorkDate, formattedWorkDate))
        conn.commit()

        # Update DEBIT trade IDs and dates in CREDIT_TRADE_TABLE
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE {CREDIT_TRADE_TABLE} ct
                SET debit_trade_id = dt.debit_trade_id, 
                    debit_trade_date = dt.debit_trade_date
                FROM {DEBIT_TRADE_TABLE} dt
                WHERE ct.credit_trade_id = dt.credit_trade_id;
            """)
        conn.commit()

        # Insert DEBIT trades into LEDGER
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {LEDGER_TABLE}(txn_id, txn_date, txn_type, amount, charges_taxes)
                SELECT debit_trade_id,
                       debit_trade_date,
                       'DEBIT',
                       trade_amount*(1+upside),
                       -1*(charges+taxes)
                FROM {DEBIT_TRADE_TABLE}
                WHERE debit_trade_date = %s::date - INTERVAL '1 day';
            """, (formattedWorkDate,))
        conn.commit()

        # Insert into CREDIT_CANDIDATE_TABLE
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {CREDIT_CANDIDATE_TABLE}

                WITH 
                existingRecordCountTable AS (
                    SELECT 
                        COUNT(*) AS existing_record_count, 
                        COUNT(CASE WHEN debit_trade_date IS NULL THEN credit_trade_id END) AS open_record_count 
                    FROM {CREDIT_TRADE_TABLE}
                ),
                
                ledgerBalanceTable AS (
                    SELECT 
                        SUM(amount + charges_taxes) AS ledger_balance 
                    FROM {LEDGER_TABLE}
                ),

                top_n_ranked AS (
                    SELECT 
                        ticker,
                        type,
                        category,
                        sector,
                        is_boh_ok,
                        is_ath_ok,
                        trade_date AS credit_trade_date,
                        closeprice AS credit_closeprice,
                        rank_used
                    FROM {TEMP_TABLE}
                    WHERE trade_date = %s::date
                    ORDER BY rank_used ASC
                    LIMIT {top_n_ranked_buy}
                ),

                open_credit_trades AS (
                    SELECT
                        ticker,
                        credit_close_price,
                        COUNT(*) AS open_cnt
                    FROM {CREDIT_TRADE_TABLE}
                    WHERE debit_trade_id IS NULL 
                    GROUP BY ticker, credit_close_price
                ),
                
                process_rank_or_average_down AS (
                    SELECT 
                        t.ticker,
                        t.type,
                        t.category,
                        t.sector,
                        t.is_boh_ok,
                        t.is_ath_ok,
                        t.credit_trade_date,
                        t.credit_closeprice,
                        t.rank_used,
                        CASE WHEN oct.open_cnt IS NULL THEN '0. First Time' 
                             ELSE CONCAT(oct.open_cnt::text, '. Average Down ') END AS process_used,
                        CASE WHEN oct.open_cnt IS NULL THEN 0 
                             ELSE oct.open_cnt END AS process_rank,
                        CASE oct.open_cnt
                            WHEN 1 THEN {avgDownThreshold1}
                            WHEN 2 THEN {avgDownThreshold2}
                            WHEN 3 THEN {avgDownThreshold3}
                        END as avgDownThreshold,
                        CASE WHEN oct.open_cnt IS NULL OR oct.open_cnt <= 3 THEN true 
                             ELSE false END AS process_qualification,
                        CASE WHEN oct.open_cnt IS NULL THEN true 
                             ELSE t.credit_closeprice < (oct.credit_close_price * avgDownThreshold) END AS process_condition
                    FROM top_n_ranked t
                    LEFT JOIN open_credit_trades oct ON t.ticker = oct.ticker
                    WHERE process_qualification AND process_condition
                ),
                                
                final_selection AS (
                    SELECT 
                        ticker,
                        type,
                        category,
                        sector,
                        is_boh_ok,
                        is_ath_ok,
                        credit_trade_date,
                        credit_closeprice,
                        rank_used,
                        process_used,
                        process_rank,
                        ROW_NUMBER() OVER (PARTITION BY credit_trade_date ORDER BY process_rank, rank_used) AS trade_row_num
                    FROM process_rank_or_average_down
                    ORDER BY process_rank, rank_used 
                )
                                
                SELECT 
                    CONCAT(credit_trade_date::text, ' | ', ticker) AS credit_trade_id,
                    credit_trade_date,
                    NULL AS debit_trade_id,
                    NULL AS debit_trade_date,
                    ticker,
                    type,
                    category,
                    sector,
                    is_boh_ok,
                    is_ath_ok,
                    credit_closeprice, 
                    process_used, 
                    process_rank,
                    rank_used,
                    FLOOR((existing_record_count + trade_row_num - 1) / {tradeSplits}) + 1 as trade_level,
                    MOD((existing_record_count + trade_row_num - 1), {tradeSplits}) + 1 as trade_split,
                    {baseTradeAmount}*POWER((1+{tradeIncrementRate}),(trade_level - 1)) as base_amount,
                    CEIL(base_amount / credit_closeprice) as trade_units,
                    trade_units * credit_closeprice as trade_amount,
                    credit_closeprice AS trade_close_price, 
                    process_used AS trade_process_used, 
                    ({fixedBuyCost} + {variableBuyCost} * trade_amount ) as charges,
                    CONCAT('RECO #',trade_row_num) AS reco
                FROM final_selection, existingRecordCountTable, ledgerBalanceTable
                WHERE open_record_count <= {openTradesLimit}
                  AND ledger_balance > trade_amount;
            """, (formattedWorkDate,))
        conn.commit()

        # Insert into CREDIT_TRADE_TABLE
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {CREDIT_TRADE_TABLE}

                WITH 
                existingRecordCountTable AS (
                    SELECT 
                        COUNT(*) AS existing_record_count, 
                        COUNT(CASE WHEN debit_trade_date IS NULL THEN credit_trade_id END) AS open_record_count 
                    FROM {CREDIT_TRADE_TABLE}
                ),

                ledgerBalanceTable AS (
                    SELECT 
                        SUM(amount + charges_taxes) AS ledger_balance 
                    FROM {LEDGER_TABLE}
                ),

                top_n_ranked AS (
                    SELECT 
                        ticker,
                        type,
                        category,
                        sector,
                        is_boh_ok,
                        is_ath_ok,
                        trade_date AS credit_trade_date,
                        closeprice AS credit_closeprice,
                        rank_used
                    FROM {TEMP_TABLE}
                    WHERE trade_date = %s::date
                    ORDER BY rank_used ASC
                    LIMIT {top_n_ranked_buy}
                ),
                
                open_credit_trades AS (
                    SELECT
                        ticker,
                        credit_close_price,
                        COUNT(*) AS open_cnt
                    FROM {CREDIT_TRADE_TABLE}
                    WHERE debit_trade_id IS NULL 
                    GROUP BY ticker, credit_close_price
                ),
                
                process_rank_or_average_down AS (
                    SELECT 
                        t.ticker,
                        t.type,
                        t.category,
                        t.sector,
                        t.is_boh_ok,
                        t.is_ath_ok,
                        t.credit_trade_date,
                        t.credit_closeprice,
                        t.rank_used,
                        CASE WHEN oct.open_cnt IS NULL THEN '0. First Time' 
                             ELSE CONCAT(oct.open_cnt::text, '. Average Down ') END AS process_used,
                        CASE WHEN oct.open_cnt IS NULL THEN 0 
                             ELSE oct.open_cnt END AS process_rank,
                        CASE oct.open_cnt
                            WHEN 1 THEN {avgDownThreshold1}
                            WHEN 2 THEN {avgDownThreshold2}
                            WHEN 3 THEN {avgDownThreshold3}
                        END as avgDownThreshold,
                        CASE WHEN oct.open_cnt IS NULL OR oct.open_cnt <= 3 THEN true 
                             ELSE false END AS process_qualification,
                        CASE WHEN oct.open_cnt IS NULL THEN true 
                             ELSE t.credit_closeprice < (oct.credit_close_price * avgDownThreshold) END AS process_condition       
                    FROM top_n_ranked t
                    LEFT JOIN open_credit_trades oct ON t.ticker = oct.ticker
                    WHERE process_qualification AND process_condition
                ),
                                
                final_selection AS (
                    SELECT 
                        ticker,
                        type,
                        category,
                        sector,
                        is_boh_ok,
                        is_ath_ok,
                        credit_trade_date,
                        credit_closeprice,
                        rank_used,
                        process_used,
                        process_rank,
                        ROW_NUMBER() OVER (PARTITION BY credit_trade_date ORDER BY process_rank, rank_used) AS trade_row_num
                    FROM process_rank_or_average_down
                    ORDER BY process_rank, rank_used
                    LIMIT {TRADES_PER_DAY}
                )
                                
                SELECT 
                    CONCAT(credit_trade_date::text, ' | ', ticker) AS credit_trade_id,
                    credit_trade_date,
                    NULL as debit_trade_id,
                    NULL as debit_trade_date,
                    ticker,
                    type,
                    category,
                    sector,
                    is_boh_ok,
                    is_ath_ok,
                    credit_closeprice,
                    process_used,
                    process_rank,
                    rank_used,
                    FLOOR((existing_record_count + trade_row_num - 1) / {tradeSplits}) + 1 as trade_level,
                    MOD((existing_record_count + trade_row_num - 1), {tradeSplits}) + 1 as trade_split,
                    {baseTradeAmount}*POWER((1+{tradeIncrementRate}),(trade_level - 1)) as base_amount,
                    CEIL(base_amount / credit_closeprice) as trade_units,
                    trade_units * credit_closeprice as trade_amount,
                    credit_closeprice AS trade_close_price, 
                    process_used AS trade_process_used, 
                    ({fixedBuyCost} + {variableBuyCost} * trade_amount ) as charges
                FROM final_selection, existingRecordCountTable, ledgerBalanceTable
                WHERE open_record_count <= {openTradesLimit}
                  AND ledger_balance > trade_amount;
            """, (formattedWorkDate,))
        conn.commit()

        # Update credit trade table close price and process used for averaging down
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE {CREDIT_TRADE_TABLE} t_old
                SET credit_close_price = new_data.close_price,
                    process_used = new_data.process_used
                FROM (
                    SELECT 
                        t_new.ticker,
                        SUM(t_new.trade_amount) / NULLIF(SUM(t_new.trade_units), 0) AS close_price,
                        MAX(t_new.process_used) AS process_used
                    FROM (
                        SELECT ticker, trade_amount, trade_units, process_used 
                        FROM {CREDIT_TRADE_TABLE} 
                        WHERE ticker IN (
                            SELECT ticker 
                            FROM {CREDIT_TRADE_TABLE} 
                            WHERE credit_trade_date = %s::date
                        )
                        AND debit_trade_id IS NULL
                    ) t_new
                    GROUP BY t_new.ticker
                ) AS new_data
                WHERE t_old.ticker = new_data.ticker
                  AND t_old.debit_trade_id IS NULL;
            """, (formattedWorkDate,))
        conn.commit()

        # Insert into LEDGER for newly inserted CREDIT trades
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {LEDGER_TABLE}(txn_id, txn_date, txn_type, amount, charges_taxes)
                SELECT credit_trade_id, credit_trade_date, 'CREDIT', -1*trade_amount, -1*charges
                FROM {CREDIT_TRADE_TABLE}
                WHERE credit_trade_date = %s::date;    
            """, (formattedWorkDate,))
        conn.commit()

        # Move to the next day
        current_date += timedelta(days=1)

    logging.info("Daily processing completed.")






########################################
# Main Execution Flow
########################################

def main():
    is_incremental, start_date, end_date, rank_column_suffix, top_n_ranked_buy, TRADES_PER_DAY, Base_Trade_Amount = parse_arguments()

    conn = get_connection()

    # Validate and create tables if not exists
    create_tables_if_not_exists(conn)

    # Truncate tables at start if needed
    truncate_tables_initial(conn, is_incremental)

    # Adjust dates if incremental
    adj_start, adj_end = get_incremental_dates(conn, is_incremental, start_date, end_date)
    if adj_start is None or adj_end is None:
        logging.error("No data to process or invalid date range.")
        conn.close()
        sys.exit(1)

    start_date, end_date = adj_start, adj_end

    # Create temp table
    create_temp_table(conn, start_date, end_date, rank_column_suffix)

    # Insert initial ledger entry
    insert_initial_ledger_entry(conn, start_date)

    # Process daily logic
    process_days(conn, start_date, end_date, top_n_ranked_buy, TRADES_PER_DAY, Base_Trade_Amount, rank_column_suffix, is_incremental)

    logging.info("Processing Completed")

    conn.close()

if __name__ == "__main__":
    main()
