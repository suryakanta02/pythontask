from utils.logger import get_logger

logger = get_logger('transform')

def transform_data(records, table_name):
    """
    Apply any necessary transformations to the data
    In this simple example, we're just passing through the data
    but you could add transformations here as needed
    """
    # Example: Convert datetime fields, clean strings, etc.
    # if table_name == 'customers':
    #     for record in records:
    #         record['name'] = record['name'].strip().title()
    
    logger.info(f"Transformed {len(records)} records from {table_name}")
    return records