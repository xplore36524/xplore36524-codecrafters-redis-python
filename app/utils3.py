sorted_set = {}

def zadd(info):
    key = info[0]
    score = info[1]
    member = info[2]

    if key not in sorted_set:
        sorted_set[key] = []

    sorted_set[key].append((score, member))

    sorted_set[key].sort(key=lambda x: x[0])

    return len(sorted_set[key])