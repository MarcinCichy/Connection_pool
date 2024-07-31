import psycopg2
from psycopg2 import connect as pg_connect
from connection_pool.server_package.config import db_config


class ConnectionPool:
    def __init__(self, minconn, maxconn):
        self.minconn = minconn
        self.maxconn = maxconn
        self.all_connections = []
        self.in_use_con = 0
        self.initailize_pool()

    def initialize_pool(self):
        for _ in range(self.minconn):
            self.all_connections.append(self.create_new_connection())

    def create_new_connection(self):
        params = db_config()
        return pg_connect(**params)






