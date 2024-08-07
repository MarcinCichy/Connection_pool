import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from connection_pool.server_package.connect import connect, release_connection, handle_connection_error, info
from psycopg2 import sql

# Liczba równoległych wątków
NUM_THREADS = 50

# Czas trwania testu w sekundach
TEST_DURATION = 300  # 5 minut

def stress_test_operation(thread_id):
    start_time = time.time()
    while time.time() - start_time < TEST_DURATION:
        conn = connect()
        try:
            with conn.cursor() as cur:
                # Losowa operacja na bazie danych
                operation = random.choice(["insert", "select"])
                if operation == "insert":
                    query = sql.SQL("INSERT INTO items (item_name, item_quantity) VALUES (%s, %s)")
                    cur.execute(query, (f'Item {random.randint(1, 100000)}', random.randint(1, 100)))
                elif operation == "select":
                    query = sql.SQL("SELECT * FROM items ORDER BY item_id DESC LIMIT 1")
                    cur.execute(query)
                    result = cur.fetchone()
                    if result:
                        print(f"Thread {thread_id}: {result}")
                conn.commit()
        except Exception as e:
            handle_connection_error(conn)
            print(f"Thread {thread_id} encountered an error: {e}")
        finally:
            release_connection(conn)
        # Krótkie opóźnienie, aby uniknąć zbyt dużej liczby operacji na sekundę
        time.sleep(random.uniform(0.01, 0.1))

def run_stress_test():
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(stress_test_operation, thread_id) for thread_id in range(NUM_THREADS)]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error occurred: {e}")
    duration = time.time() - start_time
    print(f"Stress test completed in {duration} seconds")

if __name__ == "__main__":
    run_stress_test()