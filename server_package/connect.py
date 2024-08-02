import os
import psycopg2
from psycopg2 import connect as pg_connect
from connection_pool.server_package.config import db_config
from connection_pool.server_package.conn_pool import ConnectionPool
# from tests.unit.test_config import test_db_config


class DatabaseConnectionError(Exception):
    pass


pool = ConnectionPool(minconn=5, maxconn=100)


def connect():
    try:
        return pool.aquire()
    except (Exception) as e:
        raise DatabaseConnectionError(f"Connect error = {e}")


def release_connection(conn):
    pool.release(conn)


def handle_connection_error(conn):
    pool.handle_connection_error(conn)

