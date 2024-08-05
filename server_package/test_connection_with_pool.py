import time
from connection_pool.server_package.connect import connect, release_connection, handle_connection_error, info
from psycopg2 import sql


def test_connection():
    for i in range(10000):
        conn = connect()
        try:
            with conn.cursor() as cur:
                with conn.cursor() as cur:
                    query = sql.SQL("INSERT INTO items (item_name, item_quantity ) VALUES (%s, %s)")
                    cur.execute(query, (f'Item {i}', 25))
                    conn.commit()
                info()
        except Exception as e:
            handle_connection_error(conn)
            info()
            raise e
        finally:
            release_connection(conn)
            info()


if __name__ == "__main__":
    start_time = time.time()
    test_connection()
    duration = time.time() - start_time
    print(f"Duration {duration} seconds")