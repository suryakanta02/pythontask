import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def get_db_connection():
    """Create and return a PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            database=os.getenv('DB_NAME', 'skillslab'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'password'),
            port=os.getenv('DB_PORT', '5432')
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise