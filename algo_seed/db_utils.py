# db_utils.py
import psycopg2
from psycopg2 import sql
from contextlib import contextmanager
from config import load_config

@contextmanager
def get_db_connection(db_config=None):
    if db_config is None:
        db_config, _ = load_config()
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            port="5432",
            database=db_config['name'],
            user=db_config['user'],
            password=db_config['password']
        )
        yield conn
    except psycopg2.Error as e:
        raise e
    finally:
        if conn:
            conn.close()

def table_exists(cursor, table_name):
    cursor.execute(sql.SQL("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_name = %s
        );
    """), (table_name,))
    return cursor.fetchone()[0]

def create_table(cursor, table_name, columns, unique_constraints=None, indexes=None):
    """
    Creates a table with specified columns, unique constraints, and indexes.
    
    :param cursor: Database cursor.
    :param table_name: Name of the table to create.
    :param columns: Dictionary of column names and their types.
    :param unique_constraints: List of columns to set as unique constraints.
    :param indexes: List of columns to index.
    """
    column_definitions = ", ".join([f"{col} {col_type}" for col, col_type in columns.items()])
    
    constraints = ""
    if unique_constraints:
        constraints = f", CONSTRAINT pk_{table_name} PRIMARY KEY ({', '.join(unique_constraints)})"
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {column_definitions}
        {constraints}
    );
    """
    cursor.execute(create_table_query)
    
    # Create indexes
    if indexes:
        for column in indexes:
            index_query = sql.SQL("""
                CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({column});
            """).format(
                index_name=sql.Identifier(f"idx_{column}_{table_name}"),
                table=sql.Identifier(table_name),
                column=sql.Identifier(column)
            )
            cursor.execute(index_query)
