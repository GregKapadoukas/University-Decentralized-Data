import hashlib
import random
import socket
import time


# Abstract class to be inherited by CordNode and PastryNode classes
class Node:
    hash_size = 16 * 8
    hash_max_num = 2**hash_size

    def __init__(self, host, port):
        self.id = int(hashlib.md5((host + str(port)).encode()).hexdigest(), 16)
        self.host = host
        self.port = port

    def start_node(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"Node {self.id} listening on {self.host}:{self.port}")
        while True:
            try:
                peer_socket, peer_addr = self.server_socket.accept()
                peer_socket.settimeout(2.0)
                result = self.handleCommands(peer_socket)
                if result == "close":
                    break
            except Exception as e:
                print("Timeout reached")
                time.sleep(random.uniform(1.0, 3.0))
                continue

    def handleCommands(self, peer_socket):
        pass
