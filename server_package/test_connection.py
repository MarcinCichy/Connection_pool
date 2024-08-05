import time
from connection_pool.server_package.connect_p import connect
# from connection_pool.server_package.database_support import DatabaseSupport
from psycopg2 import sql


def test_connection():
    for i in range(10000):
        try:
            # Nawiązanie połączenia z bazą danych
            conn = connect()
            if conn:
                with conn.cursor() as cur:
                    query = sql.SQL("INSERT INTO items (item_name, item_quantity ) VALUES (%s, %s)")
                    cur.execute(query, (f'Item {i}', 25))
                    conn.commit()
        except Exception as e:
            print(f"Wystąpił błąd podczas łączenia z bazą danych: {e}")


if __name__ == "__main__":
    start_time = time.time()
    test_connection()
    duration = time.time() - start_time
    print(f"Duration {duration} seconds")