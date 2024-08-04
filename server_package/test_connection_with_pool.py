import time
from connection_pool.server_package.connect import connect, release_connection, handle_connection_error


def test_connection():
    for _ in range(10000):
        conn = connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                db_version = cur.fetchone()
                print(f"Wersja bazy danych: {db_version}")
        except Exception as e:
            handle_connection_error(conn)
            raise e
        finally:
            release_connection(conn)


if __name__ == "__main__":
    start_time = time.time()
    test_connection()
    duration = time.time() - start_time
    print(f"Duration {duration} seconds")