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
        self.threads_waiting = 0
        self.total_threads = 0
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
        conn = pg_connect(**params)
        print(f"[CREATE] New connection created: {conn}")
        return conn

    def acquire(self):
        print("[DEBUG] Attempting to acquire semaphore...")
        with self.lock:
            self.threads_waiting += 1
            self.total_threads += 1
        if not self.semaphore.acquire(timeout=10):
            with self.lock:
                self.threads_waiting -= 1
            raise Exception("Failed to acquire a connection: Timeout")

        with self.lock:
            self.threads_waiting -= 1
            if self.all_connections:
                conn = self.all_connections.pop()
                print(f"[ACQUIRE] Connection acquired from pool: {conn}")
            else:
                conn = self.create_new_connection()
                print(f"[ACQUIRE] New connection created and acquired: {conn}")
            self.in_use_conn += 1
            return conn

    def release(self, conn):
        with self.lock:
            if not conn:
                print("[RELEASE ERROR] No connection provided to release.")
                return
            if self.is_connection_closed(conn):
                print(f"[RELEASE ERROR] Connection is already closed and will not be released: {conn}")
                return

            self.in_use_conn -= 1
            if not self.is_connection_closed(conn):
                if len(self.all_connections) < self.maxconn:
                    self.all_connections.append(conn)
                    print(f"[RELEASE] Connection added back to pool: {conn}")
                else:
                    self.close_connection(conn)
                    print(f"[RELEASE] Connection closed as pool is full: {conn}")
            else:
                print(f"[RELEASE] Connection was already closed and not added to pool: {conn}")

            self.semaphore.release()

    def handle_connection_error(self, conn):
        print(f"[ERROR] Handling connection error for: {conn}")
        with self.lock:
            if conn and not self.is_connection_closed(conn):
                self.in_use_conn -= 1
                self.close_connection(conn)
                print(f"[ERROR] Connection closed due to error: {conn}")

            if len(self.all_connections) < self.maxconn:
                new_conn = self.create_new_connection()
                self.all_connections.append(new_conn)
                print(f"[ERROR] Replaced bad connection with a new one: {new_conn}")

            self.semaphore.release()

    def cleanup_pool(self):
        with self.lock:
            print(f"[CLEANUP] Starting cleanup. Available connections before cleanup: {len(self.all_connections)}")
            while len(self.all_connections) > self.minconn:
                conn = self.all_connections.pop()
                self.close_connection(conn)
            print(f"[CLEANUP] Cleanup finished. Available connections after cleanup: {len(self.all_connections)}")

    def close_connection(self, conn):
        try:
            if not self.is_connection_closed(conn):
                conn.close()
                print(f"[CLOSE CONNECTION] Connection closed: {conn}")
            else:
                print(f"[CLOSE CONNECTION] Connection was already closed: {conn}")
        except Exception as e:
            print(f"[CLOSE CONNECTION ERROR] Error closing connection: {e}")

    def is_connection_closed(self, conn):
        return conn.closed

    def info(self):
        with self.lock:
            total_connections = self.in_use_conn + len(self.all_connections)
            print(f"[INFO] In use: {self.in_use_conn}, Available: {len(self.all_connections)}, Total: {total_connections}, "
                  f"Threads waiting: {self.threads_waiting}, Total threads: {self.total_threads}")

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
        """Perform cleanup asynchronously."""
        threading.Thread(target=self.cleanup_pool).start()