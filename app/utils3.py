sorted_set = {}

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
        for i in range(len(sorted_set[key])):
            if sorted_set[key][i][1] == member:
                return i
    return None