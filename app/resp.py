def resp_parser(data):
    if data.startswith(b"*"):
        parts = data.split(b"\r\n")
        num_elements = int(parts[0][1:])
        elements = []
        index = 1
        for _ in range(num_elements):
            length = int(parts[index][1:])
            element = parts[index + 1][:length].decode()
            print(f"Parsed element: {element}")
            elements.append(element)
            index += 2
        return elements
    return []

def resp_encoder(data):
    if data is None:
        return b"$-1\r\n"
    elif isinstance(data, list):
        return_string = "*"
        return_string += str(len(data)) + "\r\n"
        for item in data:
            return_string += "$"
            return_string += str(len(item)) + "\r\n"
            return_string += item + "\r\n"
        return return_string.encode()
    elif isinstance(data, str):
        return_string = "$"
        return_string += str(len(data)) + "\r\n"
        return_string += data + "\r\n"
        return return_string.encode()
    elif isinstance(data, int):
        return_string = ":" + str(data) + "\r\n"
        return return_string.encode()
    return b"$-1\r\n"

def simple_string_encoder(message):
    return f"+{message}\r\n".encode()