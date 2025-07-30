#Sketching out the class
from queue import Queue #by default bounded if provided the maxsize param

class ConnectionPool:
    def __init__(self, min_size, max_size) -> None:
        self.min_size = min_size
        self.max_size = max_size
        self.queue = Queue(max_size)
    
    def put(self, conn):
        self.queue.put(conn)