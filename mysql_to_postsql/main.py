from config.mysql_config import TABLES_TO_EXTRACT, BATCH_SIZE
from etl.extract import fetch_data_in_batches
from etl.transform import transform_data
from etl.load import load_data_to_postgres
from utils.logger import get_logger

logger = get_logger('main')

def process_table(table_name, primary_key):
    """Process a single table from MySQL to PostgreSQL"""
    logger.info(f"Starting processing for table: {table_name}")
    
    last_id = 0
    total_records = 0
    
    while True:
        # Extract
        records, last_id = fetch_data_in_batches(table_name, primary_key, last_id)
        if not records:
            break
            
        # Transform
        transformed_records = transform_data(records, table_name)
        
        # Load
        load_data_to_postgres(table_name, transformed_records)
        
        total_records += len(records)
        logger.info(f"Processed {len(records)} records (total: {total_records}) for {table_name}")
        
        # If we got fewer records than batch size, we've reached the end
        if len(records) < BATCH_SIZE:
            break
    
    logger.info(f"Finished processing {total_records} records for {table_name}")

def main():
    """Main function to process all tables"""
    try:
        for table_name, primary_key in TABLES_TO_EXTRACT.items():
            process_table(table_name, primary_key)
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise

if __name__ == "__main__":
    logger.info("Starting ETL process from MySQL to PostgreSQL")
    main()
    logger.info("ETL process completed successfully")