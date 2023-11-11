import hashlib
import socket


# Abstract class to be inherited by CordNode and PastryNode classes
class Node:
    def __init__(self, host, port):
        self.identifier = hashlib.md5((host + str(port)).encode()).hexdigest()
        self.host = host
        self.port = port
        self.__successor = None
        self.__predecessor = None
        self.__finger_table = None

    def start_node(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"Node {self.identifier} listening on {self.host}:{self.port}")
        while True:
            peer_socket, peer_addr = self.server_socket.accept()
            try:
                result = self.__handleCommands(peer_socket)
                if result == "close":
                    break
            except Exception as e:
                print(f"Error: {e}")

    def __handleCommands(self, peer_socket):
        while True:
            command = peer_socket.recv(1024).decode("utf-8").split()
            match command[0]:
                case "leave":
                    # Add communication to successor and __predecessor
                    # Inform requester
                    peer_socket.send("close".encode("utf-8"))
                    return "close"
                case "print":
                    print(command[1])
                    peer_socket.send("keep".encode("utf-8"))
                case _:
                    peer_socket.send("invalid".encode("utf-8"))
