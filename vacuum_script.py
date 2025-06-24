# import psycopg2


# conn = psycopg2.connect(
#     host="localhost",
#     database="testdb",
#     user="postgres",         
#     password="password", 
#     port=5432
# )

# conn.autocommit = True
# cur = conn.cursor()

# fetch_query = """
# SELECT schemaname, relname
# FROM pg_stat_user_tables
# WHERE n_dead_tup > 1000
# ORDER BY last_autovacuum DESC;
# """

# cur.execute(fetch_query)
# tables = cur.fetchall()

# print("Found", len(tables), "tables with dead tuples > 1000.")

# for schemaname, relname in tables:
#     full_table_name = f"{schemaname}.{relname}"
#     vacuum_query = f"VACUUM ANALYZE {full_table_name};"
    
#     print("Running:", vacuum_query)
    
#     try:
#         cur.execute(vacuum_query)
#         print("Vacuumed", full_table_name)
#     except Exception as e:
#         print("Failed to vacuum", full_table_name, ":", e)

# cur.close()
# conn.close()
# print("Done.")

# new way to write with out function 

# import os
# import psycopg2
# import logging
# from dotenv import load_dotenv


# load_dotenv()

# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
    
# )


# DB_CONFIG = {
#     "host": os.getenv("DB_HOST"),
#     "database": os.getenv("DB_NAME"),
#     "user": os.getenv("DB_USER"),
#     "password": os.getenv("DB_PASSWORD"),
#     "port": os.getenv("DB_PORT")
# }
# logging.info("new work")

# try:
#     conn = psycopg2.connect(**DB_CONFIG)
#     conn.autocommit = True
#     cur = conn.cursor()

#     fetch_query = """
#     SELECT schemaname, relname
#     FROM pg_stat_user_tables
#     WHERE n_dead_tup > 1000
#     ORDER BY last_autovacuum DESC;
#     """

#     cur.execute(fetch_query)
#     tables = cur.fetchall()

#     logging.info("Found %d tables with dead tuples > 1000.", len(tables))

#     for schemaname, relname in tables:
#         full_table_name = f"{schemaname}.{relname}"
#         vacuum_query = f"VACUUM ANALYZE {full_table_name};"

#         logging.info("Running: %s", vacuum_query)

#         try:
#             cur.execute(vacuum_query)
#             logging.info("Vacuumed %s", full_table_name)
#         except Exception as e:
#             logging.error("Failed to vacuum %s: %s", full_table_name, e)

#     cur.close()
#     conn.close()
#     logging.info("Done.")

# except Exception as conn_error:
#     logging.critical("Database connection failed: %s", conn_error)
