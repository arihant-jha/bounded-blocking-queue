#Sketching out the class
import psycopg2
import os
from queue import Queue #by default bounded if provided the maxsize param
from concurrent.futures import ThreadPoolExecutor

class ConnectionPool:
    def __init__(self, min_size, max_size, db_config) -> None:
        self.db_config = db_config
        self.min_size = min_size
        self.max_size = max_size
        self.connections = 0
        self.queue = Queue(max_size)

        # Creating initial connections - I want 5 min connections to prevent cold start
        for _ in range(5):
            conn = self._get_new_connection()
            self.queue.put(conn)

    def _get_new_connection(self):
        if self.connections < self.max_size:
            self.connections += 1
            conn = psycopg2.connect(**self.db_config)
            return conn
        return None

    def get(self):
        "Returns a connection string when it becomes available"
        # return self.queue.get()
        if self.queue.not_empty():
            return self.queue.get()
        conn = self._get_new_connection()
        if conn is not None:
            return conn
    
    def put(self, conn):
        self.queue.put(conn)
   
   
def simmulate_query(conn):
    cur = conn.cursor()
    cur.execute("SELECT pg_sleep(0.001)")
    return True
     
def test_connection_pool():
    DB_CONFIG = {
        "dbname": os.environ["PG_DB"],
        "user": os.environ["PG_USER"],
        "password": os.environ["PG_PASSWORD"],
        "host": "localhost",
        "port": int(os.environ["PG_PORT"]),
    }
    pool = ConnectionPool(5, 100, DB_CONFIG)
    with ThreadPoolExecutor(max_workers=500) as exec:
        for _ in range(500):
            futures = exec.submit(simmulate_query, pool.get)
            print(futures.result())

            
test_connection_pool()