import os
import psycopg2
from psycopg2 import connect as pg_connect
from server_package.config import db_config
# from tests.unit.test_config import test_db_config


class DatabaseConnectionError(Exception):
    pass


def connect():
    try:
        print('Connecting to the PostgreSQL database...')
        if os.getenv('TEST_ENV') == 'test':
            params = test_db_config()
        else:
            params = db_config()
        return pg_connect(**params)
    except (Exception, psycopg2.DatabaseError) as e:
       #  print(f"Connect error = {e}")
        raise DatabaseConnectionError(f"Connect error = {e}")

