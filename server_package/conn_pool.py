from psycopg2 import connect as pg_connect
from connection_pool.server_package.config import db_config


class ConnectionPool:
    def __init__(self, minconn, maxconn):
        self.minconn = minconn
        self.maxconn = maxconn
        self.all_connections = []
        self.in_use_conn = 0
        self.initialize_pool()

    def initialize_pool(self):
        for _ in range(self.minconn):
            self.all_connections.append(self.create_new_connection())

    def create_new_connection(self):
        params = db_config()
        return pg_connect(**params)

    def aquire(self):
        if self.all_connections:
            conn = self.all_connections.pop()
            self.in_use_conn += 1
            return conn
        elif self.in_use_conn < self.maxconn:
            self.in_use_conn += 1
            return self.create_new_connection()
        else:
            raise Exception("Max connections limit reached")

    def release(self, conn):
        self.in_use_conn -= 1
        self.all_connections.append(conn)

    def handle_connection_error(self, conn):
        self.in_use_conn -= 1
        conn.close()
        if len(self.all_connections) < self.minconn:
            self.all_connections.append(self.create_new_connection())










