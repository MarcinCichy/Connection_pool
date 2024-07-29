from connection_pool.server_package.connect import connect
# from server_package.database_support import DatabaseSupport

def test_connection():
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
    test_connection()