from queue import Queue

q = Queue(5)
q.put(1)
q.put(2)
print(q.qsize())
r = q.get()
print("r: ", r)
print(q.qsize())