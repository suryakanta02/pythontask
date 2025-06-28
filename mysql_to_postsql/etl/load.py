import psycopg2
from psycopg2 import Error
from psycopg2.extras import execute_batch
from config.postgres_config import POSTGRES_CONFIG, TABLE_SCHEMAS
from utils.logger import get_logger

logger = get_logger('load')

def get_postgres_connection():
    """Create and return a PostgreSQL connection"""
    try:
        connection = psycopg2.connect(**POSTGRES_CONFIG)
        return connection
    except Error as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        raise

def ensure_table_exists(connection, table_name):
    """Create table if it doesn't exist"""
    cursor = connection.cursor()
    try:
        if table_name in TABLE_SCHEMAS:
            cursor.execute(TABLE_SCHEMAS[table_name]['create_table_sql'])
            connection.commit()
            logger.info(f"Ensured table {table_name} exists")
    except Error as e:
        connection.rollback()
        logger.error(f"Error creating table {table_name}: {e}")
        raise
    finally:
        cursor.close()

def load_data_to_postgres(table_name, records):
    """Load a batch of records to PostgreSQL"""
    if not records:
        return
    
    connection = get_postgres_connection()
    ensure_table_exists(connection, table_name)
    
    cursor = connection.cursor()
    try:
        # Get column names from the first record
        columns = list(records[0].keys())
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        insert_sql = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING
        """
        
        # Convert records to list of tuples in correct column order
        values = [tuple(record[col] for col in columns) for record in records]
        
        # Use execute_batch for better performance
        execute_batch(cursor, insert_sql, values)
        connection.commit()
        
        logger.info(f"Loaded {len(records)} records to {table_name}")
        
    except Error as e:
        connection.rollback()
        logger.error(f"Error loading data to {table_name}: {e}")
        raise
    finally:
        cursor.close()
        connection.close()