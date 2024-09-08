from connection_pool.server_package.conn_pool import ConnectionManager, ConnectionCleanupTask
from connection_pool.server_package.config import connection_pool_config
from connection_pool.server_package.logger_config import setup_logger

logger = setup_logger('connect_logger')


class DatabaseConnectionError(Exception):
    pass


params = connection_pool_config()
manager = ConnectionManager(params['minconn'], params['maxconn'], params['timeout'])
cleanup_task = ConnectionCleanupTask(manager, params['cleanup_interval'])


def connect():
    try:
        return manager.acquire()
    except Exception as e:
        logger.error(f"[CONNECT ERROR] Failed to acquire connection: {e}")
        raise DatabaseConnectionError(f"Connect error = {e}")


def release_connection(conn):
    try:
        manager.release(conn)
    except Exception as e:
        logger.error(f"[RELEASE ERROR] Failed to release connection: {e}")
        raise e


def handle_connection_error(conn):
    try:
        manager.handle_connection_error(conn)
    except Exception as e:
        logger.error(f"[HANDLE ERROR] Failed to handle connection error: {e}")
        raise e


def info():
    manager.info()


def close_all_connections():
    try:
        manager.close_all_connections()
    except Exception as e:
        logger.error(f"[CLOSE ERROR] Failed to close all connections: {e}")
        raise e