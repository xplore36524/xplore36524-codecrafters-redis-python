import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    connection, _ = server_socket.accept()
    print(connection)

    # Send a response to the client
    while True:
        data = connection.recv(1024)
        if not data:
            break
        print(f"Received data: {data}")
        # if data.startswith(b"PING"):
        connection.sendall(b"+PONG\r\n")
        # elif data.startswith(b"QUIT"):
        #     connection.sendall(b"+OK\r\n")
        #     break


if __name__ == "__main__":
    main()
