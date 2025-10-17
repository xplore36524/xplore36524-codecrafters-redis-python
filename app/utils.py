import time
import threading
def expire_key(key, expire_time):
    """Expires a key after a certain time."""
    time.sleep(expire_time)
    if key in store:
        del store[key]
        
store = {}
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
