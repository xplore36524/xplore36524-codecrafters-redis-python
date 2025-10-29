sorted_set = {}
from app.geo import encode, decode, geohashGetDistance, haversine_distance
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

    for member in members:
        score_float = zscore([key, member])

        if score_float == -1:
            # Member or key does not exist: Null Array (*-1\r\n)
            final_response_parts.append(b"*-1\r\n")
            continue

        # Logic for FOUND member
        score_int = int(score_float)

        # Returns (longitude, latitude)
        try:
            longitude, latitude = decode(score_int)
        except Exception:
            # Internal error during decoding
            final_response_parts.append(b"*-1\r\n")
            continue

        # 4. Format coordinates as RESP Bulk Strings (Reverted to robust float string conversion)

        # Use Python's default high-precision float string representation (str()),
        # which is the most reliable way to maintain precision and avoid fragility.
        lon_str = str(longitude)
        lat_str = str(latitude)

        # Format as Bulk Strings
        lon_bytes = lon_str.encode()
        lat_bytes = lat_str.encode()
        lon_resp = (
            b"$" + str(len(lon_bytes)).encode() + b"\r\n" + lon_bytes + b"\r\n"
        )
        lat_resp = (
            b"$" + str(len(lat_bytes)).encode() + b"\r\n" + lat_bytes + b"\r\n"
        )

        # Final response for an existing member: *2\r\n<lon_resp><lat_resp>
        member_resp = b"*2\r\n" + lon_resp + lat_resp
        final_response_parts.append(member_resp)

    # 5. Wrap all individual responses in the final RESP array
    response = (
        b"*"
        + str(len(final_response_parts)).encode()
        + b"\r\n"
        + b"".join(final_response_parts)
    )
    return response

def geodist(info):
    key = info[0]
    member1 = info[1]
    member2 = info[2]
    if key in sorted_set:
        for i in range(len(sorted_set[key])):
            if sorted_set[key][i][1] == member1:
                longitude1, latitude1 = decode(int(sorted_set[key][i][0]))
            if sorted_set[key][i][1] == member2:
                longitude2, latitude2 = decode(int(sorted_set[key][i][0]))
        return geohashGetDistance(longitude1, latitude1, longitude2, latitude2)
    return -1

def convert_to_meters(radius: float, unit: str) -> float:
    """Converts a radius value from a given unit to meters."""
    unit = unit.lower()
    if unit == "m":
        return radius
    elif unit == "km":
        return radius * 1000.0
    elif unit == "mi":
        # 1 mile = 1609.344 meters (Redis constant)
        return radius * 1609.344
    elif unit == "ft":
        # 1 foot = 0.3048 meters
        return radius * 0.3048
    else:
        raise ValueError("Invalid unit specified")
    

def geomembers(key,center_lon,center_lat,search_radius_m):
    # 2. Get all members in the GeoKey (Sorted Set)
    if key not in sorted_set:
        return []
    members_scores = sorted_set[key]
    print(f"members_scores: {members_scores}")
    matching_members = []

    # 3. Iterate, decode coordinates, and check distance
    for score_float, member_name in members_scores:
        try:
            # Decode score to get location coordinates: returns (longitude, latitude)
            member_lon, member_lat = decode(int(score_float))
        except Exception:
            # Skip member if decoding fails
            continue

        # Calculate distance between search center and member
        distance = haversine_distance(
            center_lon, center_lat, member_lon, member_lat
        )

        # Check if the member is within the search radius (distance <= radius in meters)
        if distance <= search_radius_m:
            matching_members.append(member_name)
    print(f"matching_members: {matching_members}")
    return matching_members