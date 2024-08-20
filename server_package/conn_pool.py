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
        params = db_config()
        return pg_connect(**params)

    def aquire(self):
        if not self.semaphore.acquire(timeout=10):  # Timeout na 10 sekund, aby zapobiec deadlockom
            raise Exception("Failed to acquire a connection: Timeout")

        try:
            with self.lock:
                self.cleanup_if_needed()
                if self.all_connections:
                    conn = self.all_connections.pop()
                else:
                    conn = self.create_new_connection()
                self.in_use_conn += 1
                print(
                    f"[AQUIRE] Acquired connection. In use: {self.in_use_conn}, Available: {len(self.all_connections)}")
                return conn
        except Exception as e:
            print(f"[AQUIRE ERROR] Error while acquiring connection: {e}")
            raise e  # Ponownie rzuć wyjątek po zapisaniu błędu
        finally:
            if 'conn' not in locals() or conn is None:
                # Jeśli nie udało się przejąć połączenia, semafor powinien zostać zwolniony
                self.semaphore.release()
                print("[DEBUG] Semaphore released due to error in acquiring connection.")

    def release(self, conn):
        print("[DEBUG] Releasing connection...")
        try:
            with self.lock:
                if self.in_use_conn > 0:
                    self.in_use_conn -= 1
                    if not conn.closed:
                        if len(self.all_connections) < self.maxconn:
                            self.all_connections.append(conn)
                        else:
                            conn.close()
                    else:
                        print("[RELEASE] Connection was already closed.")
                else:
                    print("[RELEASE ERROR] No connections in use to release.")
        except Exception as e:
            print(f"[RELEASE ERROR] Exception while releasing connection: {e}")
        finally:
            self.semaphore.release()
            print(f"[DEBUG] Semaphore released. In use: {self.in_use_conn}, Available: {len(self.all_connections)}")
            self.cleanup_pool()  # Automatyczne czyszczenie puli

    def handle_connection_error(self, conn):
        try:
            with self.lock:
                if self.in_use_conn > 0:
                    self.in_use_conn -= 1
                    try:
                        conn.close()
                    except Exception as e:
                        print(f"[ERROR] Error closing connection: {e}")
                    finally:
                        if len(self.all_connections) < self.minconn:
                            self.all_connections.append(self.create_new_connection())
                    print(
                        f"[ERROR] Handled connection error. In use: {self.in_use_conn}, Available: {len(self.all_connections)}")
                else:
                    raise Exception("No connections in use to handle error.")
        finally:
            self.semaphore.release()
            print("[DEBUG] Semaphore released.")

    def cleanup_if_needed(self):
        current_time = time.time()
        if current_time - self.last_cleanup_time >= self.cleanup_interval:
            print("[DEBUG] Performing cleanup...")
            self.cleanup_pool()
            self.last_cleanup_time = current_time

    def cleanup_pool(self):
        with self.lock:
            print(f"[CLEANUP] Starting cleanup. Available connections before cleanup: {len(self.all_connections)}")
            while len(self.all_connections) > self.minconn:
                conn = self.all_connections.pop()
                try:
                    conn.close()
                    print("[CLEANUP] Closed an idle connection.")
                except Exception as e:
                    print(f"[CLEANUP ERROR] Error closing connection: {e}")
            print(f"[CLEANUP] Cleanup finished. Available connections after cleanup: {len(self.all_connections)}")

    def close_connection(self, conn):
        try:
            conn.close()
            print("[CLEANUP] Connection closed.")
        except Exception as e:
            print(f"[CLEANUP ERROR] Error closing connection: {e}")

    def info(self):
        with self.lock:
            total_connections = self.in_use_conn + len(self.all_connections)
            print(
                f"[INFO] In use: {self.in_use_conn}, Available: {len(self.all_connections)}, Total: {total_connections}")