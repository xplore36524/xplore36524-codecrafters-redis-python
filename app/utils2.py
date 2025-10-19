

#####################    STREAM OPERATIONS     #####################
streams={}

def checker(key, id_time, id_sequence):
    """Checks if the provided id_time and id_sequence are valid integers."""
    # id_time should be greater than last timestamp
    # id_sequence should be greater than last id if timestamp is same
    if(id_time=="0" and id_sequence=="0"):
        return "ERR The ID specified in XADD must be greater than 0-0"
    last_entry = streams.get(key, [])
    if last_entry:
        last_id, last_sequence = last_entry[-1].get("id").split("-")
        if int(id_time) < int(last_id):
            return "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        elif int(id_time) == int(last_id) and int(id_sequence) <= int(last_sequence):
            return "ERR The ID specified in XADD is equal or smaller than the target stream top item"
    return "ok"


def xadd(info):
    """Adds an entry to a stream."""
    key = info[0]
    id = info[1]
    id_time, id_sequence = id.split("-")
    check = checker(key, id_time, id_sequence)
    if check != "ok":
        return "err", check
    field_values = info[2:]

    if key not in streams:
        streams[key] = []

    entry = {"id": id}
    for i in range(0, len(field_values), 2):
        field = field_values[i]
        value = field_values[i + 1]
        entry[field] = value

    streams[key].append(entry)
    return "id", id  # In a real Redis implementation, this would be the entry ID

def type_getter_streams(key):
    """Returns the type of the value stored at key."""
    if key in streams:
        return "stream"
    else:
        return "none"