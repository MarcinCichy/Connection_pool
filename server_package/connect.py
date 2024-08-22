from connection_pool.server_package.conn_pool import ConnectionPool

pool = ConnectionPool(minconn=5, maxconn=100, cleanup_interval=300)

def connect():
    try:
        return pool.acquire()
    except Exception as e:
        print(f"[CONNECT ERROR] Failed to acquire connection: {e}")
        raise e

def release_connection(conn):
    try:
        pool.release(conn)
    except Exception as e:
        print(f"[RELEASE ERROR] Failed to release connection: {e}")
        raise e

def handle_connection_error(conn):
    try:
        pool.handle_connection_error(conn)
    except Exception as e:
        print(f"[HANDLE ERROR] Failed to handle connection error: {e}")
        raise e

def info():
    pool.info()

