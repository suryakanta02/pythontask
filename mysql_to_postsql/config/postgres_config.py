POSTGRES_CONFIG = {
    'host': 'localhost',
    'user': 'postgres',
    'password': 'password',
    'database': 'target_db',
    'port': 5432
}

# Target table schemas (if different from source)
TABLE_SCHEMAS = {
    'customers': {
        'columns': ['customer_id', 'name', 'email', 'created_at'],
        'create_table_sql': """
            CREATE TABLE IF NOT EXISTS customers (
                customer_id INT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                created_at TIMESTAMP
            )
        """
    },
    'orders': {
        'columns': ['order_id', 'name', 'email', 'created_at'],
        'create_table_sql': """
            CREATE TABLE IF NOT EXISTS orders (
                order_id INT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                created_at TIMESTAMP
            )
        """
    },
    'products': {
        'columns': ['product_id', 'name', 'email', 'created_at'],
        'create_table_sql': """
            CREATE TABLE IF NOT EXISTS products (
                product_id INT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                created_at TIMESTAMP
            )
        """
    },
    # Add other tables as needed
}