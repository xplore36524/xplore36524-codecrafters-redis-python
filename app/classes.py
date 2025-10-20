import socket
import threading
from app.handler import handle_client

class Master():
    def __init__(self, args):
        self.slaves = {}
        self.args = args
        self.config = {}
        self.config['role'] = 'master'

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(("localhost", int(self.args.port)))
        server_socket.listen(10)

        while True:
            client_socket, _ = server_socket.accept()
            client_thread = threading.Thread(target=handle_client, args=(client_socket,self.config))
            client_thread.daemon = True
            client_thread.start()

class Slave():
    def __init__(self, args):
        self.args = args
        self.config = {}
        self.config['role'] = 'slave'

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(("localhost", int(self.args.port)))
        server_socket.listen(10)

        while True:
            client_socket, _ = server_socket.accept()
            client_thread = threading.Thread(target=handle_client, args=(client_socket,self.config))
            client_thread.daemon = True
            client_thread.start()