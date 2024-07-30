import time
from connection_pool.server_package.connect import connect
from connection_pool.server_package.database_support import DatabaseSupport


def test_connection():
    for _ in range(10000):
        try:
            # Nawiązanie połączenia z bazą danych
            conn = connect()
            if conn:
                print("Połączenie z bazą danych zostało nawiązane pomyślnie.")
                # Wykonanie prostego zapytania
                with conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    db_version = cur.fetchone()
                    print(f"Wersja bazy danych: {db_version}")
                conn.close()
        except Exception as e:
            print(f"Wystąpił błąd podczas łączenia z bazą danych: {e}")


if __name__ == "__main__":
    start_time = time.time()
    test_connection()
    duration = time.time() - start_time
    print(f"Duration {duration} seconds")