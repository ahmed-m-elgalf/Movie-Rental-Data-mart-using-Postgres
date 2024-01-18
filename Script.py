import psycopg2


def connect(database_params):
    try:
        conn = psycopg2.connect(**database_params)
        conn.autocommit = True  
        return conn, conn.cursor()
    except Exception as e:
        log_error(e)
        raise

def log_error(error):
    with open('error_log.txt', 'a') as log_file:
        log_file.write(f"Error: {error}\n")
    print(f"Error: {error}")

def execute_query(conn, query):
    try:
        cur.execute(query)
    except Exception as e:
        log_error(e)
        raise


db_params = {
    'dbname': '####',
    'user': '####',
    'password': '####',
    'host': '####',
    'port': '####'
}


try:
    conn, cur = connect(db_params)

    with open('Queries_final.sql', 'r') as file:
        queries = file.read().split(';')

    for query in queries:
        if query.strip():
            execute_query(conn, query)

except Exception as e:
    log_error(e)

finally:
    if conn:
        conn.close()
