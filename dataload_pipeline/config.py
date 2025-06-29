import os
from dotenv import load_dotenv
import logging.config
from pathlib import Path

# Load environment variables
load_dotenv()

# Project root
BASE_DIR = Path(__file__).parent

# Database configurations
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),
    'database': os.getenv('MYSQL_DATABASE'),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD')
}

POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DATABASE'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD')
}

# Pipeline configuration
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))

# Setup logging
logging.config.fileConfig(BASE_DIR / 'logging.conf')
logger = logging.getLogger('pipeline')