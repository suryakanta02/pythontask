import mysql.connector
from mysql.connector import Error
from config.mysql_config import MYSQL_CONFIG, TABLES_TO_EXTRACT, BATCH_SIZE
from utils.logger import get_logger


logger = get_logger('extract')

def get_mysql_connection():
    """Create and return a MySQL connection"""
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        return connection
    except Error as e:
        logger.error(f"Error connecting to MySQL: {e}")
        raise

def fetch_data_in_batches(table_name, primary_key, last_id=0):
    """
    Fetch data from MySQL in batches
    Returns: list of tuples (records), last processed id
    """
    connection = get_mysql_connection()
    cursor = connection.cursor(dictionary=True)
    
    try:
        query = f"""
            SELECT * FROM {table_name} 
            WHERE {primary_key} > %s
            ORDER BY {primary_key}
            LIMIT %s
        """
        cursor.execute(query, (last_id, BATCH_SIZE))
        records = cursor.fetchall()
        
        if records:
            last_id = records[-1][primary_key]
        
        return records, last_id
        
    except Error as e:
        logger.error(f"Error fetching data from {table_name}: {e}")
        raise
    finally:
        cursor.close()
        connection.close()