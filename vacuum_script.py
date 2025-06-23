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
