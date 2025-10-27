MIN_LATITUDE = -85.05112878
MAX_LATITUDE = 85.05112878
MIN_LONGITUDE = -180
MAX_LONGITUDE = 180

LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE
LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE

def interleave(x: int, y: int) -> int:
    # First, the values are spread from 32-bit to 64-bit integers.
    # This is done by inserting 32 zero bits in-between.
    #
    # Before spread: x1  x2  ...  x31  x32
    # After spread:  0   x1  ...   0   x16  ... 0  x31  0  x32
    x = spread_int32_to_int64(x)
    y = spread_int32_to_int64(y)

    # The y value is then shifted 1 bit to the left.
    # Before shift: 0   y1   0   y2 ... 0   y31   0   y32
    # After shift:  y1   0   y2 ... 0   y31   0   y32   0
    y_shifted = y << 1

    # Next, x and y_shifted are combined using a bitwise OR.
    #
    # Before bitwise OR (x): 0   x1   0   x2   ...  0   x31    0   x32
    # Before bitwise OR (y): y1  0    y2  0    ...  y31  0    y32   0
    # After bitwise OR     : y1  x2   y2  x2   ...  y31  x31  y32  x32
    return x | y_shifted

# Spreads a 32-bit integer to a 64-bit integer by inserting
# 32 zero bits in-between.
#
# Before spread: x1  x2  ...  x31  x32
# After spread:  0   x1  ...   0   x16  ... 0  x31  0  x32
def spread_int32_to_int64(v: int) -> int:
    # Ensure only lower 32 bits are non-zero.
    v = v & 0xFFFFFFFF

    # Bitwise operations to spread 32 bits into 64 bits with zeros in-between
    v = (v | (v << 16)) & 0x0000FFFF0000FFFF
    v = (v | (v << 8))  & 0x00FF00FF00FF00FF
    v = (v | (v << 4))  & 0x0F0F0F0F0F0F0F0F
    v = (v | (v << 2))  & 0x3333333333333333
    v = (v | (v << 1))  & 0x5555555555555555

    return v

def normalize_coordinates(latitude, longitude):
    normalized_latitude = 2^26 * (latitude - MIN_LATITUDE) / LATITUDE_RANGE
    normalized_longitude = 2^26 * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE 

    normalized_latitude = int(normalized_latitude)
    normalized_longitude = int(normalized_longitude)

    score = interleave(normalized_latitude, normalized_longitude)
    return score
