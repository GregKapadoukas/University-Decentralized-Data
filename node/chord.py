import hashlib
import pickle
import socket

from node.node import Node


class FingerNodeInfo:
    def __init__(self, id, host, port):
        self.id = id
        self.host = host
        self.port = port


class FingerEntry:
    def __init__(self, start, interval, id, host, port):
        self.start = start
        self.interval = interval
        self.node = FingerNodeInfo(id, host, port)


class ChordNode(Node):
    def __init__(self, host, port):
        super().__init__(host, port)
        self.__finger_table = []
        self.__predecessor = None
        i = 1
        for i in range(1, Node.hash_size):  # 16*8 because MD5 has is 16 bytes
            self.__finger_table.append(
                FingerEntry(
                    start=self.id + 2 ^ (i - 1) % Node.hash_max_num,
                    interval=range(
                        self.id + 2 ^ (i - 1) % Node.hash_max_num,
                        self.id + 2 ^ (i) % Node.hash_max_num,
                    ),
                    id=None,
                    host=None,
                    port=None,
                )
            )

    def handleCommands(self, peer_socket):
        while True:
            command = peer_socket.recv(1024).decode("utf-8").split()
            match command[0]:
                case "leave":
                    # Add communication to successor and __predecessor
                    # Inform requester
                    peer_socket.send("close".encode("utf-8"))
                    peer_socket.close()
                    return "close"
                case "print":
                    print(command[1])
                    peer_socket.send("keep".encode("utf-8"))
                case "find_successor":
                    self.__find_sucessor(command[1])
                    peer_socket.send("keep".encode("utf-8"))
                case "find_predecessor":
                    self.__find_predecessor(command[1])
                    peer_socket.send("keep".encode("utf-8"))
                case "closest_preceeding_finger":
                    node = self.__closest_preceeding_finger(int(command[1]))
                    peer_socket.sendall(pickle.dumps(node))
                    peer_socket.close()
                    return "continue"
                case "get_your_sucessor":
                    peer_socket.sendall(pickle.dumps(self.__finger_table[0].node))
                    peer_socket.close()
                    return "continue"
                case "join":
                    self.__join(command[1], command[2])
                    peer_socket.send("keep".encode("utf-8"))
                case _:
                    peer_socket.send("invalid".encode("utf-8"))

    def __find_sucessor(self, id: int):
        n = self.__find_predecessor(id)
        n_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        n_socket.connect((n.host, n.port))
        n_socket.send("get_your_sucessor".encode("utf-8"))
        sucessor = pickle.loads(n_socket.recv(1024))
        n_socket.close()
        return sucessor

    def __find_predecessor(self, id: int):
        if id in range(
            self.id + 1, self.__finger_table[0].node.id + 0
        ):  # id not in (self.id, self.successor], [self.id+1, self.successor+1)
            return FingerNodeInfo(self.id, self.host, self.port)
        n = self.__closest_preceeding_finger(id)
        while True:
            n_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            n_socket.connect((n.host, n.port))
            n_socket.send(f"closest_preceeding_finger {id}".encode("utf-8"))
            n = pickle.loads(n_socket.recv(1024))
            n_socket.close()
            if id in range(n.id + 1, n.id + 1):
                break
        return n

    def __closest_preceeding_finger(self, id: int):
        for i in range(Node.hash_size, 0, -1):
            if self.__finger_table[i].node.id in range(
                self.id + 1, id
            ):  # node.id in (self.id, id), thus [self.id+1, id)
                return self.__finger_table[i].node
        return FingerNodeInfo(self.id, self.host, self.port)

    def __join(self, host: str, port: int):
        ifOfInviter = hashlib.md5((host + str(port)).encode()).hexdigest()
        pass
