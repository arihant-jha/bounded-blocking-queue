'''
1. initializing the max connections at the start
2. python stopped thread creation at 10k threads.
3. But the experiment ran successfully!
'''
import time
from queue import Queue
import psycopg2
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager


class ConnectionPool:
    def __init__(self, db_config, max_size) -> None:
        self.db_config = db_config
        self.max_size = max_size
        self.queue = Queue(max_size)

        for _ in range(max_size):
            conn = self._get_new_connection()
            self.queue.put(conn)

    def get(self):
        return self.queue.get()
        
    def put(self, conn):
        self.queue.put(conn)

    def _get_new_connection(self):
        conn = psycopg2.connect(**self.db_config)
        return conn

    @contextmanager
    def acquire(self):
        conn = self.get()
        try:
            yield conn
        finally:
            self.put(conn)
    
def get_pool(max_size):
    DB_CONFIG = {
            "dbname": os.environ["PG_DB"],
            "user": os.environ["PG_USER"],
            "password": os.environ["PG_PASSWORD"],
            "host": "localhost",
            "port": int(os.environ["PG_PORT"]),
        }
    pool = ConnectionPool(db_config=DB_CONFIG, max_size=max_size)
    return pool
   
def simmulate_query(conn):
    cur = conn.cursor()
    cur.execute("SELECT pg_sleep(0.001)")
    return True



def worker(pool):
    with pool.acquire() as conn:
        simmulate_query(conn)
    
def test_connection_pool():
    parallel_threads = 9000
    max_size = 200
    
    pool = get_pool(max_size)
    start_time = time.time()


    with ThreadPoolExecutor(max_workers=parallel_threads) as exec:
        # conn = pool.get()
        # exec.submit(simmulate_query, conn)
        # pool.put(conn)
        futures = [exec.submit(worker, pool) for _ in range(parallel_threads)]
        for f in futures:
            f.result()
    
    end_time = time.time()
    print(end_time - start_time)

    
if __name__ == "__main__":
    # pool = get_pool()
    # simmulate_query(pool.get())
    test_connection_pool()