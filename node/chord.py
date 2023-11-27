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
        self.__predecessor = FingerNodeInfo(None, None, None)
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
                    self.__find_successor(command[1])
                    peer_socket.send("keep".encode("utf-8"))
                case "find_predecessor":
                    self.__find_predecessor(command[1])
                    peer_socket.send("keep".encode("utf-8"))
                case "closest_preceeding_finger":
                    node = self.__closest_preceeding_finger(int(command[1]))
                    peer_socket.sendall(pickle.dumps(node))
                case "get_your_successor":
                    peer_socket.sendall(pickle.dumps(self.__finger_table[0].node))
                case "initialize_network":
                    self.__initialize_network()
                    peer_socket.send("keep".encode("utf-8"))
                case "join":
                    self.__join(command[1], command[2])
                    peer_socket.send("keep".encode("utf-8"))
                case "update_finger_table":
                    self.__update_finger_table(int(command[1]), int(command[2]))
                    peer_socket.send("keep".encode("utf-8"))
                case "close":
                    peer_socket.close()
                    return "continue"
                case _:
                    peer_socket.send("invalid".encode("utf-8"))

    def __find_successor(self, id: int):
        n = self.__find_predecessor(id)
        n_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        n_socket.connect((n.host, n.port))
        n_socket.send("get_your_successor".encode("utf-8"))
        successor = pickle.loads(n_socket.recv(1024))
        n_socket.send("close".encode("utf-8"))
        n_socket.close()
        return successor

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
            n_socket.send("close".encode("utf-8"))
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

    def __initialize_network(self):
        n = FingerNodeInfo(self.id, self.host, self.port)
        for i in range(1, Node.hash_size):
            self.__finger_table[i].node = n
        self.__predecessor = n

    def __join(self, inviter_host: str, inviter_port: int):
        self.__init_finger_table(inviter_host, inviter_port)
        self.__update_others()
        # move keys in (predecessor, n] from successor

    def __init_finger_table(self, inviter_host: str, inviter_port: int):
        inviter_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        inviter_socket.connect((inviter_host, inviter_port))
        inviter_socket.send(
            f"find_successor {self.__finger_table[0].start}".encode("utf-8")
        )
        self.__finger_table[0].node = pickle.loads(inviter_socket.recv(1024))
        for i in range(1, Node.hash_size - 1):
            if self.__finger_table[i + 1].start in range(
                self.id, self.__finger_table[i].interval
            ):
                self.__finger_table[i + 1].node = self.__finger_table[i].node
            else:
                inviter_socket.send(
                    f"find_successor {self.__finger_table[i+1].start}".encode("utf-8")
                )
                self.__finger_table[i + 1].node = pickle.loads(
                    inviter_socket.recv(1024)
                )
        inviter_socket.send("close".encode("utf-8"))
        inviter_socket.close()

    def __update_others(self):
        for i in range(1, Node.hash_size):
            p = self.__find_predecessor(self.id - 2 ^ (i - 1))
            p_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            p_socket.connect((p.host, p.port))
            p_socket.send(f"update_finger_table {self.id} {i}".encode("utf-8"))
            _ = p_socket.recv(1024).decode("utf-8")
            p_socket.send("close".encode("utf-8"))
            p_socket.close()

    def __update_finger_table(self, id, i):
        if id in range(self.id, self.__finger_table[i].node.id):
            self.__finger_table[i].node = id
            p_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            p_socket.connect((self.__predecessor.host, self.__predecessor.port))
            p_socket.send(f"update_finger_table {self.id} {i}".encode("utf-8"))
            _ = p_socket.recv(1024).decode("utf-8")
            p_socket.send("close".encode("utf-8"))
            p_socket.close()
