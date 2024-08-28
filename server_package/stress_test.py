import time
import random
from concurrent.futures import ThreadPoolExecutor
from connection_pool.server_package.connect import connect, release_connection, handle_connection_error, info
from connection_pool.server_package.config import stress_test
from psycopg2 import sql
import logging

# Konfiguracja loggera
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(threadName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

params = stress_test()
NUM_THREADS = int(params['num_threads'])
TEST_DURATION = int(params['test_duration'])


def stress_test_operation(thread_id):
    start_time = time.time()
    conn = None
    try:
        while time.time() - start_time < TEST_DURATION:
            try:
                conn = connect()
                with conn.cursor() as cur:
                    if random.random() < 0.1:
                        cur.execute("SELECT * FROM non_existing_table")
                        logging.warning(f"[SELECT ERROR] Thread {thread_id}: Tried to select from non-existing table.")
                    else:
                        operation = random.choice(["insert", "select"])
                        if operation == "insert":
                            query = sql.SQL("INSERT INTO items (item_name, item_quantity) VALUES (%s, %s)")
                            cur.execute(query, (f'Item {random.randint(1, 100000)}', random.randint(1, 100)))
                            logging.info(f"[INSERT] Thread {thread_id}: Inserted new item.")
                        elif operation == "select":
                            query = sql.SQL("SELECT * FROM items ORDER BY item_id DESC LIMIT 1")
                            cur.execute(query)
                            logging.info(f"[SELECT] Thread {thread_id}: Executed SELECT.")
                            result = cur.fetchone()
                            if result:
                                logging.debug(f"Thread {thread_id}: {result}")
                    conn.commit()
                info()
            except Exception as e:
                if conn:
                    handle_connection_error(conn)
                logging.error(f"Thread {thread_id} encountered an error: {e}")
            finally:
                if conn:
                    release_connection(conn)
                    conn = None
            time.sleep(random.uniform(0.01, 0.1))
    except Exception as e:
        logging.critical(f"Thread {thread_id} encountered a fatal error: {e}")


def run_stress_test():
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(stress_test_operation, thread_id) for thread_id in range(NUM_THREADS)]
        for future in futures:
            future.result()
    logging.info(f"Stress test completed in {time.time() - start_time} seconds")


if __name__ == "__main__":
    run_stress_test()
    logging.info("End of Stress Test")