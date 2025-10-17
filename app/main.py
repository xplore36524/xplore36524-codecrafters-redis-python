import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    connection, _ = server_socket.accept()

    # Send a response to the client
    connection.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()
