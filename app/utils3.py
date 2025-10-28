sorted_set = {}
from app.geo import encode, decode
# geolocations = {}

########################## SORTED SETS ##########################
def zadd(info):
    key = info[0]
    score = info[1]
    member = info[2]

    if key not in sorted_set:
        sorted_set[key] = []

    # update score if member already exists
    for i in range(len(sorted_set[key])):
        if sorted_set[key][i][1] == member:
            sorted_set[key][i] = (score, member)
            sorted_set[key].sort(key=lambda x: (x[0], x[1]))
            return 0

    # add new member
    sorted_set[key].append((score, member))
    sorted_set[key].sort(key=lambda x: (x[0], x[1]))
    return 1

def zrank(info):
    key = info[0]
    member = info[1]

    if key in sorted_set:
        sorted_set[key].sort(key=lambda x: (x[0], x[1]))
        for i in range(len(sorted_set[key])):
            if sorted_set[key][i][1] == member:
                return i
    return None

def zrange(info):
    key = info[0]
    start = int(info[1])
    end = int(info[2])
    if key not in sorted_set:
        return []
    sorted_set[key].sort(key=lambda x: (x[0], x[1]))
    print(f"zrange called with key: {key}, start: {start}, end: {end}")
    print(f"sorted_set: {sorted_set}")
    if end < 0:
        end = len(sorted_set[key]) + end
    if start < 0:
        start = len(sorted_set[key]) + start
    if start >= len(sorted_set[key]) or (end > 0 and end < start):
        return []
    start = max(start, 0)
    end = max(end, 0)
    start = min(start, len(sorted_set[key])-1)
    end = min(end, len(sorted_set[key])-1)
    result = []
    if key in sorted_set:
        for i in range(start, end+1):
            result.append(sorted_set[key][i][1])
    return result

def zcard(info):
    key = info[0]
    if key in sorted_set:
        return len(sorted_set[key])
    return 0

def zscore(info):
    key = info[0]
    member = info[1]
    if key in sorted_set:
        for i in range(len(sorted_set[key])):
            if sorted_set[key][i][1] == member:
                return sorted_set[key][i][0]
    return -1

def zrem(info):
    key = info[0]
    member = info[1]
    if key in sorted_set:
        for i in range(len(sorted_set[key])):
            if sorted_set[key][i][1] == member:
                del sorted_set[key][i]
                return 1
    return 0


#################################### GEOSPATIAL ##############################

def geoadd(info):
    key = info[0]
    longitude = float(info[1])
    latitude = float(info[2])
    member = info[3]

    if abs(longitude)>180 or abs(latitude)>85.05112878:
        return -1
    if key not in sorted_set:
        sorted_set[key] = []

    norm = encode(latitude, longitude)
    print(norm)
    norm = str(norm)
    sorted_set[key].append((norm, member))
    sorted_set[key].sort(key=lambda x: (x[0], x[1]))
    return 1

def geopos(info):
    key = info[0]
    members = info[1:]

    final_response_parts = []

    # Hardcoded coordinates as requested for this stage
    HARDCODED_LONGITUDE_STR = "0"
    HARDCODED_LATITUDE_STR = "0"

    # Pre-encode for efficiency
    lon_bytes = HARDCODED_LONGITUDE_STR.encode()
    lat_bytes = HARDCODED_LATITUDE_STR.encode()
    lon_resp = b"$" + str(len(lon_bytes)).encode() + b"\r\n" + lon_bytes + b"\r\n"
    lat_resp = b"$" + str(len(lat_bytes)).encode() + b"\r\n" + lat_bytes + b"\r\n"

    # Full response for an existing member: *2\r\n<lon_resp><lat_resp>
    MEMBER_FOUND_RESP = b"*2\r\n" + lon_resp + lat_resp
    # Full response for a missing member: Null Array
    MEMBER_MISSING_RESP = b"*-1\r\n"

    for member in members:
        # Check if the member exists in the sorted set (Geo data is stored as ZSET)
        score = zscore(key, member)

        if score is None:
            # Member or key does not exist: Null Array (*-1\r\n)
            final_response_parts.append(MEMBER_MISSING_RESP)
        else:
            # Member exists: Array of [longitude, latitude] with hardcoded values
            final_response_parts.append(MEMBER_FOUND_RESP)

    # Wrap all individual responses in the final RESP array
    response = (
        b"*"
        + str(len(final_response_parts)).encode()
        + b"\r\n"
        + b"".join(final_response_parts)
    )
    return response