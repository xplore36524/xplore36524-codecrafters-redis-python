import socket  # noqa: F401
import threading
from app.resp import resp_parser, resp_encoder
from app.utils import getter, setter, rpush
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
                response = "+PONG\r\n".encode()
            # ECHO
            elif decoded_data[0].upper() == "ECHO" and len(decoded_data) > 1:
                response = resp_encoder(decoded_data[1])
            # GET
            elif decoded_data[0].upper() == "GET":
                response = resp_encoder(getter(decoded_data[1]))
                print(f"GET response: {response}")
            # SET
            elif decoded_data[0].upper() == "SET" and len(decoded_data) > 2:
                setter(decoded_data[1:])
                response = "+OK\r\n".encode()
            # RPUSH
            elif decoded_data[0].upper() == "RPUSH" and len(decoded_data) > 2:
                # For simplicity, we treat RPUSH similar to SET in this implementation
                size = rpush(decoded_data[1:])
                response = resp_encoder(size)
            else:
                response = resp_encoder("ERR")

            connection.sendall(response)

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    while True:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(("localhost", 6379))

        server_socket.listen(10)
        client_socket, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(client_socket,)).start()


if __name__ == "__main__":
    main()
