import time
import threading
from psycopg2 import connect as pg_connect
from connection_pool.server_package.config import db_config
from connection_pool.server_package.logger_config import setup_logger

logger = setup_logger('connection_pool_logger')


class ConnectionFactory:
    @staticmethod
    def create_new_connection():
        params = db_config()
        conn = pg_connect(**params)
        logger.debug(f"New connection created: {conn}")
        return conn


class ConnectionManager:
    def __init__(self, minconn, maxconn, timeout):
        self.minconn = int(minconn)
        self.maxconn = int(maxconn)
        self.timeout = int(timeout)
        self.all_connections = []
        self.in_use_conn = 0
        self.lock = threading.Lock()
        self.semaphore = threading.BoundedSemaphore(self.maxconn)
        self._initialize_pool()

    def _initialize_pool(self):
        with self.lock:
            for _ in range(self.minconn):
                self.all_connections.append(ConnectionFactory.create_new_connection())
            logger.info(f"Initialized connection pool with {self.minconn} connections.")

    def acquire(self):
        logger.debug("Attempting to acquire semaphore...")

        if not self.semaphore.acquire(timeout=self.timeout):
            logger.error("Failed to acquire a connection: Timeout")
            raise Exception("Failed to acquire a connection: Timeout")

        conn = self._get_connection()
        return conn

    def release(self, conn):
        if not conn or self.is_connection_closed(conn):
            logger.warning(f"Connection {conn} is already closed and will not be released.")
            return

        with self.lock:
            if self.in_use_conn > 0:
                self.in_use_conn -= 1
            if len(self.all_connections) < self.maxconn:
                self.all_connections.append(conn)
                logger.info(f"Connection {conn} added back to pool.")
            else:
                self._close_connection(conn)
                logger.info(f"Connection {conn} closed as pool is full.")

        self.semaphore.release()

    def handle_connection_error(self, conn):
        with self.lock:
            if conn and not self.is_connection_closed(conn):
                self.in_use_conn -= 1
                self._close_connection(conn)
                logger.error(f"Connection {conn} closed due to error.")

            if len(self.all_connections) < self.maxconn:
                self._add_new_connection()

        self.semaphore.release()

    def _get_connection(self):
        with self.lock:
            self.in_use_conn += 1
            if self.all_connections:
                conn = self.all_connections.pop()
                logger.info(f"Connection {conn} acquired from pool.")
            else:
                conn = self._add_new_connection()
            return conn

    def _add_new_connection(self):
        conn = ConnectionFactory.create_new_connection()
        logger.info(f"New connection created and acquired: {conn}")
        return conn

    def is_connection_closed(self, conn):
        return conn.closed

    def _close_connection(self, conn):
        try:
            if not self.is_connection_closed(conn):
                conn.close()
                logger.debug(f"Connection {conn} closed.")
            else:
                logger.debug(f"Connection {conn} was already closed.")
        except Exception as e:
            logger.error(f"Error closing connection {conn}: {e}")

    def close_all_connections(self):
        with self.lock:
            logger.info("Closing all connections in the pool.")
            for conn in self.all_connections:
                self._close_connection(conn)
            self.all_connections.clear()
            logger.info("All connections have been closed.")

    def info(self):
        with self.lock:
            total_connections = self.in_use_conn + len(self.all_connections)
            logger.info(
                f"In use: {self.in_use_conn}, Available: {len(self.all_connections)}, Total: {total_connections}")


class ConnectionCleanupTask:
    def __init__(self, manager: ConnectionManager, cleanup_interval):
        self.manager = manager
        self.cleanup_interval = int(cleanup_interval)
        self.start_cleanup_thread()

    def start_cleanup_thread(self):
        cleanup_thread = threading.Thread(target=self._cleanup_task, daemon=True)
        cleanup_thread.start()

    def _cleanup_task(self):
        while True:
            time.sleep(self.cleanup_interval)
            self._cleanup_pool()

    def _cleanup_pool(self):
        with self.manager.lock:
            logger.info(f"Starting cleanup. Available connections before cleanup: {len(self.manager.all_connections)}")
            while len(self.manager.all_connections) > self.manager.minconn:
                conn = self.manager.all_connections.pop()
                self.manager._close_connection(conn)
            logger.info(f"Cleanup finished. Available connections after cleanup: {len(self.manager.all_connections)}")
