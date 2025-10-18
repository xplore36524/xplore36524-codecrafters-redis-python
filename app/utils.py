import time
import threading
def expire_key(key, expire_time):
    """Expires a key after a certain time."""
    time.sleep(expire_time)
    if key in store:
        del store[key]
        
store = {}
store_list = {}
def setter(info):
    """Sets the value for a given key in the in-memory store."""
    if len(info) == 2:
        key, value = info
        store[key] = value
    elif info[2] == "PX":
        key, value, _, expire_time = info
        store[key] = value
        # Set expiration time in milliseconds
        expire_time = int(expire_time) / 1000.0
        threading.Thread(target=expire_key, args=(key, expire_time)).start()

def getter(key):
    """Gets the value for a given key from the in-memory store."""
    return store.get(key)

def rpush(info):
    """Appends values to a list stored at key."""
    key = info[0]
    values = info[1:]
    if key not in store_list:
        store_list[key] = []
    store_list[key].extend(values)
    return len(store_list[key])

def lrange(info):
    """Returns a range of elements from a list stored at key."""
    key = info[0]
    start = int(info[1])
    end = int(info[2])
    # Adjust end for Python's slicing
    if end < 0:
        end = len(store_list[key]) + end
    if key not in store_list or start >= len(store_list[key]) or (end > 0 and end < start):
        return []
    else:
        end += 1
    return store_list[key][start:min(end, len(store_list[key]))] if end is not None else store_list[key][end]