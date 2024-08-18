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
        self.lock = threading.Lock()  # Lock do zabezpieczenia wspólnych zasobów
        self.semaphore = threading.Semaphore(self.maxconn)  # Semaphore do zarządzania dostępem
        self.initialize_pool()

    def initialize_pool(self):
        with self.lock:
            for _ in range(self.minconn):
                self.all_connections.append(self.create_new_connection())
            print(f"Initialized connection pool with {self.minconn} connections.")

    def create_new_connection(self):
        params = db_config()
        return pg_connect(**params)

    def aquire(self):
        print("[DEBUG] Attempting to acquire connection...")
        self.semaphore.acquire()  # Zajmij miejsce w semaforze
        print("[DEBUG] Semaphore acquired, entering lock...")
        with self.lock:
            print("[DEBUG] Inside lock, about to cleanup...")
            self.cleanup_if_needed()
            if self.all_connections:
                conn = self.all_connections.pop()
                self.in_use_conn += 1
                print(
                    f"[AQUIRE] Acquired connection. In use: {self.in_use_conn}, Available: {len(self.all_connections)}")
            else:
                self.in_use_conn += 1
                print(
                    f"[CREATE] Creating new connection. In use: {self.in_use_conn}, Available: {len(self.all_connections)}")
                conn = self.create_new_connection()
            print("[DEBUG] Leaving lock, returning connection.")
            return conn

    def release(self, conn):
        print("[DEBUG] Releasing connection...")
        with self.lock:
            if self.in_use_conn > 0:
                self.in_use_conn -= 1
                if len(self.all_connections) < self.maxconn:
                    if not conn.closed:
                        self.all_connections.append(conn)
                        print(
                            f"[RELEASE] Released connection. In use: {self.in_use_conn}, Available: {len(self.all_connections)}")
                    else:
                        print("[RELEASE] Connection was already closed, not adding back to the pool.")
                else:
                    print(f"[RELEASE] Pool is full, closing connection.")
                    conn.close()
            else:
                print("[RELEASE] No connections in use, something went wrong.")
        self.semaphore.release()  # Zwolnij miejsce w semaforze
        print("[DEBUG] Semaphore released.")

    def handle_connection_error(self, conn):
        with self.lock:
            if self.in_use_conn > 0:
                self.in_use_conn -= 1
                conn.close()
                if len(self.all_connections) < self.minconn:
                    self.all_connections.append(self.create_new_connection())
                print(f"[ERROR] Handled connection error. In use: {self.in_use_conn}, Available: {len(self.all_connections)}")
            else:
                print("[ERROR] No connections in use, something went wrong.")
        self.semaphore.release()  # Zwolnij miejsce w semaforze

    def cleanup_if_needed(self):
        with self.lock:
            current_time = time.time()
            if current_time - self.last_cleanup_time >= self.cleanup_interval:
                self.cleanup_pool()
                self.last_cleanup_time = current_time

    def cleanup_pool(self):
        with self.lock:
            while len(self.all_connections) > self.minconn:
                conn = self.all_connections.pop()
                conn.close()
                print("[CLEANUP] Closed an idle connection.")

    def info(self):
        with self.lock:
            total_connections = self.in_use_conn + len(self.all_connections)
            print(f"[INFO] Number of pool: {self.in_use_conn}, Available connections: {len(self.all_connections)}, Total: {total_connections}")
