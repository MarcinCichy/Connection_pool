import threading
import time
from psycopg2 import connect as pg_connect
from connection_pool.server_package.config import db_config


class ConnectionPool:
    def __init__(self, minconn, maxconn, cleanup_interval):
        self.minconn = int(minconn)
        self.maxconn = int(maxconn)
        self.cleanup_interval = int(cleanup_interval)
        self.all_connections = []
        self.in_use_conn = 0
        self.last_cleanup_time = time.time()
        self.lock = threading.Lock()
        self.semaphore = threading.BoundedSemaphore(self.maxconn)
        self.initialize_pool()

    def initialize_pool(self):
        with self.lock:
            for _ in range(self.minconn):
                self.all_connections.append(self.create_new_connection())
            print(f"Initialized connection pool with {self.minconn} connections.")

    def create_new_connection(self):
        return pg_connect(**db_config())

    def acquire(self):
        print("[DEBUG] Attempting to acquire semaphore...")
        if not self.semaphore.acquire(timeout=10):
            raise Exception("Failed to acquire a connection: Timeout")

        conn = None
        try:
            with self.lock:
                self.cleanup_if_needed()
                conn = self.all_connections.pop() if self.all_connections else self.create_new_connection()
                self.in_use_conn += 1  # Zwiększenie licznika połączeń w użyciu
                print(f"[ACQUIRE] Acquired connection. In use: {self.in_use_conn}, Available: {len(self.all_connections)}")
                return conn
        except Exception as e:
            if conn:
                self.semaphore.release()
                print("[DEBUG] Semaphore released due to error in acquiring connection.")
            raise e

    def release(self, conn):
        with self.lock:
            if conn:  # Sprawdzenie, czy połączenie istnieje
                self.in_use_conn -= 1  # Zmniejszenie licznika połączeń w użyciu
                if not self.is_connection_closed(conn):
                    if len(self.all_connections) < self.maxconn:
                        self.all_connections.append(conn)
                    else:
                        self.close_connection(conn)
                self.semaphore.release()
                print(f"[RELEASE] Connection released. In use: {self.in_use_conn}, Available: {len(self.all_connections)}")
            else:
                print("[RELEASE ERROR] Connection is None, nothing to release.")

    def handle_connection_error(self, conn):
        print("[ERROR] Handling connection error...")
        with self.lock:
            try:
                self.in_use_conn -= 1
                conn.close()  # Zamknięcie uszkodzonego połączenia
                if len(self.all_connections) < self.maxconn:
                    self.all_connections.append(self.create_new_connection())
                    print("[ERROR] Replaced bad connection with a new one.")
            except Exception as e:
                print(f"[ERROR] Failed to handle connection error: {e}")
            finally:
                self.semaphore.release()
                print(f"[DEBUG] Semaphore released after handling connection error. In use: {self.in_use_conn}, Available: {len(self.all_connections)}")

    def cleanup_if_needed(self):
        if time.time() - self.last_cleanup_time >= self.cleanup_interval:
            self.cleanup_pool()
            self.last_cleanup_time = time.time()

    def cleanup_pool(self):
        with self.lock:
            print(f"[CLEANUP] Starting cleanup. Available connections before cleanup: {len(self.all_connections)}")
            while len(self.all_connections) > self.minconn:
                conn = self.all_connections.pop()
                self.close_connection(conn)
            print(f"[CLEANUP] Cleanup finished. Available connections after cleanup: {len(self.all_connections)}")

    def close_connection(self, conn):
        try:
            conn.close()
            print("[CLEANUP] Connection closed.")
        except Exception as e:
            print(f"[CLEANUP ERROR] Error closing connection: {e}")

    def is_connection_closed(self, conn):
        return conn.closed

    def info(self):
        with self.lock:
            total_connections = self.in_use_conn
            print(f"[INFO] In use: {self.in_use_conn}, Available: {len(self.all_connections)}, Total: {total_connections}")