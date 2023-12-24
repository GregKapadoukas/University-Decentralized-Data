import hashlib
import random
import socket
import time


# Abstract class to be inherited by CordNode and PastryNode classes
class P2PNode:
    hash_size = 16 * 8
    hash_max_num = 2**hash_size

    def __init__(self, host, port):
        self.id = int(hashlib.md5((host + str(port)).encode()).hexdigest(), 16)
        self.host = host
        self.port = port
        self.data = {}

    def start_node(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"Node {self.id} listening on {self.host}:{self.port}", flush=True)
        while True:
            peer_connection, peer_addr = self.server_socket.accept()
            peer_connection.settimeout(10.0)
            result = self.handle_commands(peer_connection)
            if result == "close":
                break

    def handle_commands(self, peer_connection):
        pass

    def store_data(self, chord_key, data_key, data):
        if chord_key in self.data.keys():
            if data_key in self.data[chord_key].keys():
                self.data[chord_key][data_key].append(data)
            else:
                self.data[chord_key][data_key] = [data]
        else:
            self.data[chord_key] = {data_key: [data]}

    def get_data(self, chord_key, data_key):
        if chord_key in self.data.keys() and data_key in self.data[chord_key].keys():
            return self.data[chord_key][data_key]
        else:
            return ["Not Found"]
