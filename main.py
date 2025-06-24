import psycopg2
import logging
from dotenv import load_dotenv
import os


def load_config():
    load_dotenv()
    return {
        "host": os.getenv("DB_HOST"),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": os.getenv("DB_PORT", 5432),
    }


def setup_logging():
    logging.basicConfig(
        filename="vacuum_log.log",
        filemode="a",
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.INFO
    )
    logging.info("Logging is set up.")


def get_connection(config):
    try:
        conn = psycopg2.connect(**config)
        conn.autocommit = True
        logging.info("Connected to the database.")
        return conn
    except Exception as e:
        logging.critical(f"Database connection failed: {e}")
        raise

def find_tables_with_dead_tuples(cursor):
    query = """
    SELECT schemaname, relname
    FROM pg_stat_user_tables
    WHERE n_dead_tup > 1000
    ORDER BY last_autovacuum DESC;
    """
    cursor.execute(query)
    tables = cursor.fetchall()
    logging.info(f"Found {len(tables)} tables with dead tuples > 1000.")
    return tables


def vacuum_tables(cursor, tables):
    for schemaname, relname in tables:
        full_table_name = f"{schemaname}.{relname}"
        vacuum_query = f"VACUUM ANALYZE {full_table_name};"
        logging.info(f"Running: {vacuum_query}")
        try:
            cursor.execute(vacuum_query)
            logging.info(f"Vacuumed {full_table_name}")
        except Exception as e:
            logging.error(f"Failed to vacuum {full_table_name}: {e}")


def main():
    setup_logging()
    logging.info("Script started.")
    config = load_config()

    try:
        conn = get_connection(config)
        cur = conn.cursor()

        tables = find_tables_with_dead_tuples(cur)
        vacuum_tables(cur, tables)

        cur.close()
        conn.close()
        logging.info("Script finished successfully.")

    except Exception as e:
        logging.critical(f"Script failed: {e}")


if __name__ == "__main__":
    main()
