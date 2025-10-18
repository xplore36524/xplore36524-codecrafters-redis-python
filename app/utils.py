import time
import threading
from app.resp import resp_encoder

#####################    LIST OPERATIONS     #####################
store = {}
store_list = {}

def expire_key(key, expire_time):
    """Expires a key after a certain time."""
    time.sleep(expire_time)
    if key in store:
        del store[key]

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

def rpush(info, blocked):
    """Appends values to a list stored at key."""
    key = info[0]
    values = info[1:]
    if key not in store_list:
        store_list[key] = []
    store_list[key].extend(values)
    new_len = len(store_list[key])
    # honour blpop blocked connections
    print(blocked)
    while key in blocked and blocked[key] and len(store_list[key]) > 0:
        
        connection = blocked[key].pop(0)
        value = store_list[key].pop(0)
        response = resp_encoder([key, value])
        try:
            connection.sendall(response)
        except:
            pass

    return new_len

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

def lpush(info):
    """Prepends values to a list stored at key."""
    key = info[0]
    values = info[1:]
    if key not in store_list:
        store_list[key] = []
    for value in values:
        store_list[key].insert(0, value)
    return len(store_list[key])

def llen(key):
    """Returns the length of the list stored at key."""
    if key in store_list:
        return len(store_list[key])
    return 0

def lpop(info):
    """Removes and returns the first element of the list stored at key."""
    if len(info) == 1:
        key = info[0]
        if key in store_list and store_list[key]:
            return store_list[key].pop(0)
        return None
    elif len(info) == 2:
        # print(info)
        key = info[0]
        count = int(info[1])
        if key in store_list and store_list[key]:
            popped_elements = []
            for _ in range(min(count, len(store_list[key]))):
                popped_elements.append(store_list[key].pop(0))
            return popped_elements
        return []

def blpop(info, connection, blocked):
    """Removes and returns the first element of the list stored at key."""
    key = info[0]
    if key in store_list and len(store_list[key]) > 0:
        return [key, store_list[key].pop(0)]
    else:
        if key not in blocked:
            blocked[key] = []
    blocked[key].append(connection)
    return None


#####################    END LIST OPERATIONS     #####################

#####################    STREAM OPERATIONS     #####################