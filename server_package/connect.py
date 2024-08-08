from connection_pool.server_package.conn_pool import ConnectionPool
from connection_pool.server_package.config import connection_pool_config


class DatabaseConnectionError(Exception):
    pass


pool = ConnectionPool(minconn=5, maxconn=15)

pool.cleanup_if_needed()


def connect():
    try:
        return pool.aquire()
    except (Exception) as e:
        raise DatabaseConnectionError(f"Connect error = {e}")


def release_connection(conn):
    pool.release(conn)


def handle_connection_error(conn):
    pool.handle_connection_error(conn)


def info():
    pool.info()


