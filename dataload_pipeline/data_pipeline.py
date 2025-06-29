import mysql.connector
import psycopg2
from psycopg2.extras import execute_batch
from config import MYSQL_CONFIG, POSTGRES_CONFIG, logger, BATCH_SIZE
from typing import List, Dict, Any
import time
import re

class DataPipeline:
    def __init__(self):
        self.mysql_conn = None
        self.postgres_conn = None
    
    def connect_to_mysql(self):
        """Establish connection to MySQL database"""
        try:
            self.mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
            logger.info("Successfully connected to MySQL database")
        except mysql.connector.Error as err:
            logger.error(f"MySQL connection error: {err}")
            raise
    
    def connect_to_postgres(self):
        """Establish connection to PostgreSQL database"""
        try:
            self.postgres_conn = psycopg2.connect(**POSTGRES_CONFIG)
            logger.info("Successfully connected to PostgreSQL database")
        except psycopg2.Error as err:
            logger.error(f"PostgreSQL connection error: {err}")
            raise
    
    def extract_data(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Extract data from MySQL table
        Returns list of dictionaries where keys are column names
        """
        if not self.mysql_conn:
            self.connect_to_mysql()
        
        cursor = self.mysql_conn.cursor(dictionary=True)
        
        try:
            # Get column information
            cursor.execute(f"DESCRIBE {table_name}")
            columns = [col['Field'] for col in cursor.fetchall()]
            
            # Fetch data in batches
            cursor.execute(f"SELECT * FROM {table_name}")
            
            data = []
            while True:
                batch = cursor.fetchmany(BATCH_SIZE)
                if not batch:
                    break
                data.extend(batch)
            
            logger.info(f"Extracted {len(data)} records from {table_name}")
            return data, columns
            
        except mysql.connector.Error as err:
            logger.error(f"Error extracting data from {table_name}: {err}")
            raise
        finally:
            cursor.close()
    
    def convert_mysql_to_postgres(self, create_stmt: str) -> str:
        """
        Convert MySQL CREATE TABLE statement to PostgreSQL syntax
        """
        # Basic replacements
        create_stmt = create_stmt.replace('`', '"')
        create_stmt = re.sub(r'ENGINE=\w+', '', create_stmt)
        create_stmt = create_stmt.replace('DEFAULT CURRENT_TIMESTAMP', 'DEFAULT CURRENT_TIMESTAMP')
        
        # Handle AUTO_INCREMENT
        create_stmt = re.sub(
            r'int(?:\(\d+\))? NOT NULL AUTO_INCREMENT',
            'SERIAL PRIMARY KEY',
            create_stmt
        )
        
        # Handle other integer types
        create_stmt = re.sub(r'int\(\d+\)', 'INTEGER', create_stmt)
        create_stmt = re.sub(r'tinyint(?:\(\d+\))?', 'SMALLINT', create_stmt)
        create_stmt = re.sub(r'smallint(?:\(\d+\))?', 'SMALLINT', create_stmt)
        create_stmt = re.sub(r'bigint(?:\(\d+\))?', 'BIGINT', create_stmt)
        
        # Handle string types
        create_stmt = re.sub(r'varchar\((\d+)\)', r'VARCHAR(\1)', create_stmt)
        create_stmt = re.sub(r'char\((\d+)\)', r'CHAR(\1)', create_stmt)
        create_stmt = re.sub(r'text(?:\(\d+\))?', 'TEXT', create_stmt)
        
        # Handle decimal types
        create_stmt = re.sub(r'decimal\((\d+),(\d+)\)', r'DECIMAL(\1,\2)', create_stmt)
        
        # Handle ENUM types (convert to VARCHAR with CHECK constraint)
        def replace_enum(match):
            enum_values = match.group(1)
            # Extract the column name from the full line
            line_start = create_stmt.rfind('\n', 0, match.start()) + 1
            line_end = create_stmt.find('\n', match.start())
            line = create_stmt[line_start:line_end].strip()
            col_name = line.split('"')[1]  # Get the column name between quotes
            return f'VARCHAR(255) CHECK ("{col_name}" IN ({enum_values}))'
        
        create_stmt = re.sub(
            r'enum\((\'[^\']+\'(?:,\s*\'[^\']+\')*)\)',
            replace_enum,
            create_stmt,
            flags=re.IGNORECASE
        )
        
        # Convert UNIQUE KEY to PostgreSQL syntax
        create_stmt = re.sub(
            r'UNIQUE KEY "[^"]+" \(("[^"]+"(?:,\s*"[^"]+")*)\)',
            r'UNIQUE (\1)',
            create_stmt
        )
        
        # Convert KEY to PostgreSQL INDEX (if needed)
        create_stmt = re.sub(
            r'KEY "[^"]+" \(("[^"]+"(?:,\s*"[^"]+")*)\)',
            r'',
            create_stmt
        )
        
        # Remove MySQL-specific attributes
        create_stmt = re.sub(r'CHARACTER SET \w+', '', create_stmt)
        create_stmt = re.sub(r'COLLATE \w+', '', create_stmt)
        
        return create_stmt
    
    def create_postgres_table(self, table_name: str, columns: List[str], mysql_data: List[Dict[str, Any]]):
        """
        Create table in PostgreSQL if it doesn't exist
        """
        if not self.postgres_conn:
            self.connect_to_postgres()
        
        cursor = self.postgres_conn.cursor()
        
        try:
            # Get MySQL table structure to create similar table in PostgreSQL
            with self.mysql_conn.cursor(dictionary=True) as mysql_cursor:
                mysql_cursor.execute(f"SHOW CREATE TABLE {table_name}")
                create_table_stmt = mysql_cursor.fetchone()['Create Table']
                
                # Convert MySQL syntax to PostgreSQL
                create_table_stmt = self.convert_mysql_to_postgres(create_table_stmt)
                
                # Create the table
                cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
                cursor.execute(create_table_stmt)
                self.postgres_conn.commit()
                logger.info(f"Created table {table_name} in PostgreSQL")
                
        except (mysql.connector.Error, psycopg2.Error) as err:
            logger.error(f"Error creating table {table_name} in PostgreSQL: {err}")
            self.postgres_conn.rollback()
            raise
        finally:
            cursor.close()
    
    def load_data(self, table_name: str, data: List[Dict[str, Any]], columns: List[str]):
        """
        Load data into PostgreSQL table
        """
        if not self.postgres_conn:
            self.connect_to_postgres()
        
        cursor = self.postgres_conn.cursor()
        
        try:
            # Prepare the INSERT statement
            placeholders = ', '.join(['%s'] * len(columns))
            columns_str = ', '.join([f'"{col}"' for col in columns])
            insert_stmt = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            
            # Convert dicts to tuples in the correct column order
            data_tuples = []
            for record in data:
                try:
                    data_tuples.append(tuple(record[col] for col in columns))
                except KeyError as e:
                    logger.warning(f"Missing column {e} in record: {record}")
                    # Fill missing columns with None
                    data_tuples.append(tuple(record.get(col, None) for col in columns))
            
            # Execute in batches
            execute_batch(cursor, insert_stmt, data_tuples, page_size=BATCH_SIZE)
            self.postgres_conn.commit()
            
            logger.info(f"Loaded {len(data)} records into {table_name} in PostgreSQL")
            
        except psycopg2.Error as err:
            logger.error(f"Error loading data into {table_name}: {err}")
            self.postgres_conn.rollback()
            raise
        finally:
            cursor.close()
    
    def transfer_table(self, table_name: str):
        """Transfer data from MySQL to PostgreSQL for a single table"""
        try:
            start_time = time.time()
            
            logger.info(f"Starting transfer for table: {table_name}")
            
            # Extract data from MySQL
            data, columns = self.extract_data(table_name)
            
            # Create table in PostgreSQL
            self.create_postgres_table(table_name, columns, data)
            
            # Load data into PostgreSQL
            if data:
                self.load_data(table_name, data, columns)
            
            elapsed = time.time() - start_time
            logger.info(f"Completed transfer for {table_name} in {elapsed:.2f} seconds")
            
        except Exception as err:
            logger.error(f"Failed to transfer table {table_name}: {str(err)}")
            raise
    
    def close_connections(self):
        """Close all database connections"""
        if self.mysql_conn:
            self.mysql_conn.close()
            logger.info("Closed MySQL connection")
        if self.postgres_conn:
            self.postgres_conn.close()
            logger.info("Closed PostgreSQL connection")

def main():
    """Main function to run the data pipeline"""
    pipeline = DataPipeline()
    
    try:
        # List of tables to transfer
        tables_to_transfer = ['customers', 'orders']
        
        for table in tables_to_transfer:
            pipeline.transfer_table(table)
            
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
    finally:
        pipeline.close_connections()

if __name__ == "__main__":
    main()