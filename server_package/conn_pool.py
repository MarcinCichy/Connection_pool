import time
from psycopg2 import connect as pg_connect
from connection_pool.server_package.config import db_config


class ConnectionPool:
    def __init__(self, minconn, maxconn, cleanup_interval=60):
        self.minconn = minconn
        self.maxconn = maxconn
        self.cleanup_interval = cleanup_interval
        self.all_connections = []
        self.in_use_conn = 0
        self.last_cleanup_time = time.time()
        self.initialize_pool()

    def initialize_pool(self):
        for _ in range(self.minconn):
            self.all_connections.append(self.create_new_connection())
        print(f"Initialized connection pool with {self.minconn} connections.")

    def create_new_connection(self):
        params = db_config()
        return pg_connect(**params)

    def aquire(self):
        self.cleanup_if_needed()
        if self.all_connections:
            conn = self.all_connections.pop()
            self.in_use_conn += 1
            print(f"Acquired connection. In use: {self.in_use_conn}, Available: {len(self.all_connections)}")
            return conn
        elif self.in_use_conn < self.maxconn:
            self.in_use_conn += 1
            print(f"Creating new connection. In use: {self.in_use_conn}, Available: {len(self.all_connections)}")
            return self.create_new_connection()
        else:
            raise Exception("Max connections limit reached")

    def release(self, conn):
        if self.in_use_conn > 0:
            self.in_use_conn -= 1
            if len(self.all_connections) < self.maxconn:
                if not conn.closed:  # Dodajemy połączenie do puli tylko, jeśli jest otwarte
                    self.all_connections.append(conn)
                    print(f"Released connection. In use: {self.in_use_conn}, Available: {len(self.all_connections)}")
                else:
                    print("Connection was already closed, not adding back to the pool.")
            else:
                print(f"Pool is full, closing connection.")
                conn.close()
        else:
            print("[RELEASE] No connections in use, something went wrong.")

    def handle_connection_error(self, conn):
        if self.in_use_conn > 0:
            self.in_use_conn -= 1
            conn.close()
            if len(self.all_connections) < self.minconn:
                self.all_connections.append(self.create_new_connection())
            print(f"Handled connection error. In use: {self.in_use_conn}, Available: {len(self.all_connections)}")
        else:
            print("[ERROR] No connections in use, something went wrong.")

    def cleanup_if_needed(self):
        current_time = time.time()
        if current_time - self.last_cleanup_time >= self.cleanup_interval:
            self.cleanup_pool()
            self.last_cleanup_time = current_time

    def cleanup_pool(self):
        while len(self.all_connections) > self.minconn:
            conn = self.all_connections.pop()
            conn.close()
            print("[CLEANUP] Closed an idle connection.")

    def info(self):
        total_connections = self.in_use_conn + len(self.all_connections)
        print(f"Number of pool: {self.in_use_conn}, Available connections: {len(self.all_connections)}, Total: {total_connections}")
