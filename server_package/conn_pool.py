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
        self.lock = threading.Lock()
        self.semaphore = threading.BoundedSemaphore(self.maxconn)
        self.initialize_pool()
        self.start_cleanup_thread()

    def initialize_pool(self):
        with self.lock:
            for _ in range(self.minconn):
                self.all_connections.append(self.create_new_connection())
            print(f"Initialized connection pool with {self.minconn} connections.")

    def create_new_connection(self):
        params = db_config()
        return pg_connect(**params)

    def acquire(self):
        print("[DEBUG] Attempting to acquire semaphore...")
        if not self.semaphore.acquire(timeout=10):
            raise Exception("Failed to acquire a connection: Timeout")

        with self.lock:
            if self.all_connections:
                conn = self.all_connections.pop()
            else:
                conn = self.create_new_connection()
            self.in_use_conn += 1
            print(f"[ACQUIRE] Acquired connection. In use: {self.in_use_conn}, Available: {len(self.all_connections)}")
            return conn

    def release(self, conn):
        with self.lock:
            if not conn or self.is_connection_closed(conn):
                print(f"[RELEASE ERROR] Connection is already closed: {conn}")
                return

            if self.in_use_conn > 0:
                self.in_use_conn -= 1
                if len(self.all_connections) < self.maxconn:
                    self.all_connections.append(conn)
                else:
                    self.close_connection(conn)
                print(f"[RELEASE] Connection released. In use: {self.in_use_conn}, Available: {len(self.all_connections)}")
                self.semaphore.release()
            else:
                print(f"[RELEASE ERROR] Failed to release connection: Semaphore released too many times")

    def handle_connection_error(self, conn):
        print("[ERROR] Handling connection error...")
        with self.lock:
            if conn and not self.is_connection_closed(conn):
                self.in_use_conn -= 1
                try:
                    conn.close()
                except Exception as e:
                    print(f"[HANDLE ERROR] Failed to close connection: {e}")
                finally:
                    if len(self.all_connections) < self.maxconn:
                        self.all_connections.append(self.create_new_connection())
                        print("[ERROR] Replaced bad connection with a new one.")
                    self.semaphore.release()
                    print(f"[DEBUG] Semaphore released after handling connection error. In use: {self.in_use_conn}, Available: {len(self.all_connections)}")
            else:
                print(f"[HANDLE ERROR] Invalid connection error handling: Connection is None or already closed")

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
            total_connections = self.in_use_conn + len(self.all_connections)
            print(f"[INFO] In use: {self.in_use_conn}, Available: {len(self.all_connections)}, Total: {total_connections}")

    def maintain_minconn(self):
        """Ensure there are at least minconn connections in the pool."""
        with self.lock:
            while len(self.all_connections) < self.minconn:
                self.all_connections.append(self.create_new_connection())
            print(f"[MAINTENANCE] Ensured minimum connections. Available: {len(self.all_connections)}")

    def start_cleanup_thread(self):
        """Start a background thread that cleans up the pool periodically."""
        cleanup_thread = threading.Thread(target=self.cleanup_task, daemon=True)
        cleanup_thread.start()

    def cleanup_task(self):
        """Periodically clean up the pool to ensure it's not overpopulated."""
        while True:
            time.sleep(self.cleanup_interval)
            self.cleanup_pool_async()

    def cleanup_pool_async(self):
        threading.Thread(target=self.cleanup_pool).start()