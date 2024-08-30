import time
from psycopg2 import connect as pg_connect
from connection_pool.server_package.config import db_config
from connection_pool.server_package.logger_config import setup_logger
import threading

logger = setup_logger('connection_pool_logger')


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
            logger.info(f"Initialized connection pool with {self.minconn} connections.")

    def create_new_connection(self):
        params = db_config()
        conn = pg_connect(**params)
        logger.debug(f"New connection created: {conn}")
        return conn

    def acquire(self):
        logger.debug("Attempting to acquire semaphore...")
        with self.lock:
            self.threads_waiting += 1
            self.total_threads += 1
        if not self.semaphore.acquire(timeout=10):
            with self.lock:
                self.threads_waiting -= 1
            logger.error("Failed to acquire a connection: Timeout")
            raise Exception("Failed to acquire a connection: Timeout")

        with self.lock:
            self.threads_waiting -= 1
            if self.all_connections:
                conn = self.all_connections.pop()
                logger.info(f"Connection acquired from pool: {conn}")
            else:
                conn = self.create_new_connection()
                logger.info(f"New connection created and acquired: {conn}")
            self.in_use_conn += 1
            return conn

    def release(self, conn):
        with self.lock:
            if not conn:
                logger.warning("No connection provided to release.")
                return
            if self.is_connection_closed(conn):
                logger.warning(f"Connection is already closed and will not be released: {conn}")
                return

            self.in_use_conn -= 1
            if not self.is_connection_closed(conn):
                if len(self.all_connections) < self.maxconn:
                    self.all_connections.append(conn)
                    logger.info(f"Connection added back to pool: {conn}")
                else:
                    self.close_connection(conn)
                    logger.info(f"Connection closed as pool is full: {conn}")
            else:
                logger.info(f"Connection was already closed and not added to pool: {conn}")

            self.semaphore.release()

    def handle_connection_error(self, conn):
        logger.error(f"Handling connection error for: {conn}")
        with self.lock:
            if conn and not self.is_connection_closed(conn):
                self.in_use_conn -= 1
                self.close_connection(conn)
                logger.error(f"Connection closed due to error: {conn}")

            if len(self.all_connections) < self.maxconn:
                new_conn = self.create_new_connection()
                self.all_connections.append(new_conn)
                logger.info(f"Replaced bad connection with a new one: {new_conn}")

            self.semaphore.release()

    def cleanup_pool(self):
        with self.lock:
            logger.info(f"Starting cleanup. Available connections before cleanup: {len(self.all_connections)}")
            while len(self.all_connections) > self.minconn:
                conn = self.all_connections.pop()
                self.close_connection(conn)
            logger.info(f"Cleanup finished. Available connections after cleanup: {len(self.all_connections)}")

    def close_connection(self, conn):
        try:
            if not self.is_connection_closed(conn):
                conn.close()
                logger.debug(f"Connection closed: {conn}")
            else:
                logger.debug(f"Connection was already closed: {conn}")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")

    def is_connection_closed(self, conn):
        return conn.closed

    def info(self):
        with self.lock:
            total_connections = self.in_use_conn + len(self.all_connections)
            logger.info(f"In use: {self.in_use_conn}, Available: {len(self.all_connections)}, Total: {total_connections}, "
                         f"Threads waiting: {self.threads_waiting}, Total threads: {self.total_threads}")

    def maintain_minconn(self):
        """Ensure there are at least minconn connections in the pool."""
        with self.lock:
            while len(self.all_connections) < self.minconn:
                self.all_connections.append(self.create_new_connection())
            logger.debug(f"Ensured minimum connections. Available: {len(self.all_connections)}")

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
