from app.resp import resp_encoder
import math

MIN_LATITUDE = -85.05112878
MAX_LATITUDE = 85.05112878
MIN_LONGITUDE = -180
MAX_LONGITUDE = 180

LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE
LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE

EARTH_RADIUS_IN_METERS = 6372797.560856

def deg_rad(deg):
    """Convert degrees to radians."""
    return deg * math.pi / 180.0

def encode(latitude: float, longitude: float) -> int:
    # Normalize to the range 0-2^26
    normalized_latitude = 2**26 * (latitude - MIN_LATITUDE) / LATITUDE_RANGE
    normalized_longitude = 2**26 * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE

    # Truncate to integers
    normalized_latitude = int(normalized_latitude)
    normalized_longitude = int(normalized_longitude)

    return interleave(normalized_latitude, normalized_longitude)


def interleave(x: int, y: int) -> int:
    x = spread_int32_to_int64(x)
    y = spread_int32_to_int64(y)

    y_shifted = y << 1
    return x | y_shifted


def spread_int32_to_int64(v: int) -> int:
    v = v & 0xFFFFFFFF

    v = (v | (v << 16)) & 0x0000FFFF0000FFFF
    v = (v | (v << 8)) & 0x00FF00FF00FF00FF
    v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F
    v = (v | (v << 2)) & 0x3333333333333333
    v = (v | (v << 1)) & 0x5555555555555555

    return v

def decode(geo_code: int):
    """
    decode converts geo code(WGS84) to tuple of (latitude, longitude)
    """
    # Align bits of both latitude and longitude to take even-numbered position
    y = geo_code >> 1
    x = geo_code
    
    # Compact bits back to 32-bit ints
    grid_latitude_number = compact_int64_to_int32(x)
    grid_longitude_number = compact_int64_to_int32(y)
    
    return convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number)


def compact_int64_to_int32(v: int) -> int:
    """
    Compact a 64-bit integer with interleaved bits back to a 32-bit integer.
    This is the reverse operation of spread_int32_to_int64.
    """
    v = v & 0x5555555555555555
    v = (v | (v >> 1)) & 0x3333333333333333
    v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F
    v = (v | (v >> 4)) & 0x00FF00FF00FF00FF
    v = (v | (v >> 8)) & 0x0000FFFF0000FFFF
    v = (v | (v >> 16)) & 0x00000000FFFFFFFF
    return v


def convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number):
    # Calculate the grid boundaries
    grid_latitude_min = MIN_LATITUDE + LATITUDE_RANGE * (grid_latitude_number / (2**26))
    grid_latitude_max = MIN_LATITUDE + LATITUDE_RANGE * ((grid_latitude_number + 1) / (2**26))
    grid_longitude_min = MIN_LONGITUDE + LONGITUDE_RANGE * (grid_longitude_number / (2**26))
    grid_longitude_max = MIN_LONGITUDE + LONGITUDE_RANGE * ((grid_longitude_number + 1) / (2**26))
    
    # Calculate the center point of the grid cell
    latitude = (grid_latitude_min + grid_latitude_max) / 2
    longitude = (grid_longitude_min + grid_longitude_max) / 2
    return (longitude, latitude)

def geohashGetLatDistance(lat1d, lat2d):
    """Compute north–south distance (latitude difference only)."""
    return EARTH_RADIUS_IN_METERS * abs(deg_rad(lat2d) - deg_rad(lat1d))

def geohashGetDistance(lon1d, lat1d, lon2d, lat2d):
    """Compute great-circle distance between two coordinates."""
    lon1r = deg_rad(lon1d)
    lon2r = deg_rad(lon2d)
    v = math.sin((lon2r - lon1r) / 2.0)

    # Optimization: if longitude difference is 0, use latitude-only distance
    if v == 0.0:
        return geohashGetLatDistance(lat1d, lat2d)

    lat1r = deg_rad(lat1d)
    lat2r = deg_rad(lat2d)
    u = math.sin((lat2r - lat1r) / 2.0)
    a = u * u + math.cos(lat1r) * math.cos(lat2r) * v * v
    return 2.0 * EARTH_RADIUS_IN_METERS * math.asin(math.sqrt(a))

def haversine_distance(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Calculates the distance between two points (lon, lat) using the Haversine formula."""
    # Convert degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Differences
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    # Haversine formula calculation: a = sin²(dlat/2) + cos(lat1) * cos(lat2) * sin²(dlon/2)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    )
    # c = 2 * atan2(sqrt(a), sqrt(1-a)) simplifies to 2 * asin(sqrt(a))
    c = 2 * math.asin(math.sqrt(a))

    distance = EARTH_RADIUS_IN_METERS * c
    return distance