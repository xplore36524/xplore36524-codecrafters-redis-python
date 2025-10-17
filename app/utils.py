store = {}
def setter(key, value):
    """Sets the value for a given key in the in-memory store."""
    store[key] = value

def getter(key):
    """Gets the value for a given key from the in-memory store."""
    return store.get(key, None)
