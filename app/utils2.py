

#####################    STREAM OPERATIONS     #####################
streams={}
def xadd(info):
    """Adds an entry to a stream."""
    key = info[0]
    id = info[1]
    field_values = info[2:]

    if key not in streams:
        streams[key] = []

    entry = {"id": id}
    for i in range(0, len(field_values), 2):
        field = field_values[i]
        value = field_values[i + 1]
        entry[field] = value

    streams[key].append(entry)
    return id  # In a real Redis implementation, this would be the entry ID

def type_getter_streams(key):
    """Returns the type of the value stored at key."""
    if key in streams:
        return "stream"
    else:
        return "none"