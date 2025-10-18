import socket  # noqa: F401
import threading
from app.resp import resp_parser, resp_encoder, simple_string_encoder
from app.utils import getter, setter, rpush, lrange, lpush, llen, lpop, blpop, type_getter

blocked = {}
def handle_client(connection):
    with connection:
        while True:
            data = connection.recv(1024)
            if not data:
                break
            print(f"Received data: {data}")

            decoded_data = resp_parser(data)
            # PING
            if decoded_data[0] == "PING":
                response = simple_string_encoder("PONG")
                connection.sendall(response)
            # ECHO
            elif decoded_data[0].upper() == "ECHO" and len(decoded_data) > 1:
                response = resp_encoder(decoded_data[1])
                connection.sendall(response)
            # GET
            elif decoded_data[0].upper() == "GET":
                response = resp_encoder(getter(decoded_data[1]))
                print(f"GET response: {response}")
                connection.sendall(response)
            # SET
            elif decoded_data[0].upper() == "SET" and len(decoded_data) > 2:
                setter(decoded_data[1:])
                response = "+OK\r\n".encode()
                connection.sendall(response)
            # RPUSH
            elif decoded_data[0].upper() == "RPUSH" and len(decoded_data) > 2:
                # For simplicity, we treat RPUSH similar to SET in this implementation
                size = rpush(decoded_data[1:], blocked)
                response = resp_encoder(size)
                connection.sendall(response)
            # LRANGE
            elif decoded_data[0].upper() == "LRANGE" and len(decoded_data) > 3:
                response = resp_encoder(lrange(decoded_data[1:]))
                connection.sendall(response)
            # LPUSH
            elif decoded_data[0].upper() == "LPUSH":
                size = lpush(decoded_data[1:])
                response = resp_encoder(size)
                connection.sendall(response)
            # LLEN
            elif decoded_data[0].upper() == "LLEN" and len(decoded_data) > 1:
                size = llen(decoded_data[1])
                response = resp_encoder(size)
                connection.sendall(response)
            # LPOP
            elif decoded_data[0].upper() == "LPOP" and len(decoded_data) > 1:
                response = resp_encoder(lpop(decoded_data[1:]))
                connection.sendall(response)
            # BLPOP
            elif decoded_data[0].upper() == "BLPOP" and len(decoded_data) > 2:
                response = blpop(decoded_data[1:], connection, blocked)
                if response is None:
                    continue
                else:
                    response = resp_encoder(response)
                    connection.sendall(response)
            # TYPE
            elif decoded_data[0].upper() == "TYPE" and len(decoded_data) > 1:
                response = simple_string_encoder(type_getter(decoded_data[1]))
                connection.sendall(response)
            else:
                response = resp_encoder("ERR")
                connection.sendall(response)

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("localhost", 6379))
    server_socket.listen(10)

    while True:
        client_socket, _ = server_socket.accept()
        client_thread = threading.Thread(target=handle_client, args=(client_socket,))
        client_thread.daemon = True
        client_thread.start()


if __name__ == "__main__":
    main()
