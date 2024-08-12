import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from connection_pool.server_package.connect import connect, release_connection, handle_connection_error, info
from psycopg2 import sql

NUM_THREADS = 110
TEST_DURATION = 300  # 5 minut


def stress_test_operation(thread_id):
    start_time = time.time()
    conn = None
    while time.time() - start_time < TEST_DURATION:
        try:
            conn = connect()
            with conn.cursor() as cur:
                if random.random() < 0.1:
                    cur.execute("SELCT * FROM non_existing_table")
                    print(f"[SELCT ERROR]")
                else:
                    operation = random.choice(["insert", "select"])
                    if operation == "insert":
                        query = sql.SQL("INSERT INTO items (item_name, item_quantity) VALUES (%s, %s)")
                        cur.execute(query, (f'Item {random.randint(1, 100000)}', random.randint(1, 100)))
                        print(f"[INSERT]")
                    elif operation == "select":
                        query = sql.SQL("SELECT * FROM items ORDER BY item_id DESC LIMIT 1")
                        print(f"[SELECT]")
                        cur.execute(query)
                        result = cur.fetchone()
                        if result:
                            print(f"Thread {thread_id}: {result}")
                conn.commit()
            info()
        except Exception as e:
            if conn:
                handle_connection_error(conn)
            print(f"Thread {thread_id} encountered an error: {e}")
            conn = None
        finally:
            if conn:
                release_connection(conn)

        time.sleep(random.uniform(0.01, 0.1))

    time.sleep(TEST_DURATION * 0.1)


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