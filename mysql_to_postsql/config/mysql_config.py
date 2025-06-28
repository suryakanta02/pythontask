MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'ecommerce_db',
    'port': 3306
}


TABLES_TO_EXTRACT = {
    'customers': 'customer_id',
    'orders': 'order_id',
    'products': 'product_id'
}

BATCH_SIZE = 1000  

