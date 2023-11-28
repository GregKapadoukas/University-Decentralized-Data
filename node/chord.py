import pickle
import random
import socket
import threading
import time

from node.node import Node


class FingerUpdateSettings:
    def __init__(self, mode, stabilize_interval, fix_fingers_interval):
        self.mode = mode
        self.stabilize_interval = stabilize_interval
        self.fix_fingers_interval = fix_fingers_interval


class FingerNodeInfo:
    def __init__(self, id, host, port):
        self.id = id
        self.host = host
        self.port = port

    def __str__(self):
        return f"id: {self.id}, host {self.host}, port {self.port}"


class FingerEntry:
    def __init__(self, start, interval, id, host, port):
        self.start = start
        self.interval = interval
        self.node = FingerNodeInfo(id, host, port)

    def __str__(self):
        return f"start: {self.start}, interval {self.interval}, node {self.node}"


class ChordNode(Node):
    def __init__(
        self,
        host,
        port,
        finger_update_settings,
    ):
        super().__init__(host, port)
        self.__finger_update_mode = finger_update_settings.mode
        self.__finger_table = []
        self.__predecessor = FingerNodeInfo(None, None, None)
        i = 1
        for i in range(1, Node.hash_size + 1):  # 16*8 because MD5 has is 16 bytes
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
        if self.__finger_update_mode == "normal":
            self.__stabilize_interval = finger_update_settings.stabilize_interval
            self.__fix_fingers_interval = finger_update_settings.fix_fingers_interval
            self.__stabilize_thread = threading.Thread(target=self.__stabilize)
            self.__fix_fingers_thread = threading.Thread(target=self.__fix_fingers)
            self.__active = True

    def handleCommands(self, peer_socket):
        while True:
            command = peer_socket.recv(1024).decode("utf-8").split()
            match command[0]:
                case "leave":
                    # Add communication to successor and __predecessor
                    # Inform requester
                    if self.__finger_update_mode == "normal":
                        self.__active = False
                        self.__stabilize_thread.join()
                        self.__fix_fingers_thread.join()
                    peer_socket.send("close".encode("utf-8"))
                    peer_socket.close()
                    return "close"
                case "print":
                    print(command[1])
                    peer_socket.send("keep".encode("utf-8"))
                case "find_successor":
                    peer_socket.send(
                        pickle.dumps(self.__find_successor(int(command[1])))
                    )
                case "find_predecessor":
                    self.__find_predecessor(int(command[1]))
                    peer_socket.send("keep".encode("utf-8"))
                case "closest_preceeding_finger":
                    node = self.__closest_preceeding_finger(int(command[1]))
                    peer_socket.sendall(pickle.dumps(node))
                case "get_your_successor":
                    peer_socket.sendall(pickle.dumps(self.__finger_table[0].node))
                case "get_your_predecessor":
                    peer_socket.sendall(pickle.dumps(self.__predecessor))
                case "initialize_network":
                    self.__initialize_network()
                    peer_socket.send("keep".encode("utf-8"))
                case "join":
                    self.__join(command[1], int(command[2]))
                    peer_socket.send("keep".encode("utf-8"))
                case "update_finger_table":
                    self.__update_finger_table(int(command[1]), int(command[2]))
                    peer_socket.send("keep".encode("utf-8"))
                case "notify":
                    self.__notify(int(command[1]), str(command[2]), int(command[3]))
                    peer_socket.send("keep".encode("utf-8"))
                case "close":
                    peer_socket.close()
                    return "continue"
                case _:
                    peer_socket.send("invalid".encode("utf-8"))

    def __find_successor(self, id: int):
        n = self.__find_predecessor(id)
        if n.id is not self.id:
            n_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            n_socket.connect((n.host, n.port))
            n_socket.send("get_your_successor".encode("utf-8"))
            successor = pickle.loads(n_socket.recv(1024))
            n_socket.send("close".encode("utf-8"))
            n_socket.close()
        else:
            successor = n
        return successor

    def __find_predecessor(self, id: int):
        if id in range(
            self.id + 1, self.__finger_table[0].node.id + 1
        ):  # id not in (self.id, self.successor], [self.id+1, self.successor+1)
            return FingerNodeInfo(self.id, self.host, self.port)
        n = self.__closest_preceeding_finger(id)
        if n.id is not self.id:
            while True:
                n_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                n_socket.connect((n.host, n.port))
                n_socket.send(f"closest_preceeding_finger {id}".encode("utf-8"))
                n = pickle.loads(n_socket.recv(1024))
                n_socket.send("get_your_successor".encode("utf-8"))
                successor = pickle.loads(n_socket.recv(1024))
                n_socket.send("close".encode("utf-8"))
                n_socket.close()
                if id in range(n.id + 1, successor + 1):  # id in (n.id, sucessor]
                    break
        return n

    def __closest_preceeding_finger(self, id: int):
        for i in range(Node.hash_size - 1, 0, -1):
            if self.__finger_table[i].node.id in range(
                self.id + 1, id
            ):  # node.id in (self.id, id), thus [self.id+1, id)
                return self.__finger_table[i].node
        return FingerNodeInfo(self.id, self.host, self.port)

    def __initialize_network(self):
        n = FingerNodeInfo(self.id, self.host, self.port)
        for i in range(0, Node.hash_size):
            self.__finger_table[i].node = n
        self.__predecessor = n

    def __join(self, inviter_host: str, inviter_port: int):
        if self.__finger_update_mode == "aggressive":
            self.__predecessor = FingerNodeInfo(self.id, self.host, self.port)
            # self.__predecessor = None
            inviter_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            inviter_socket.connect((inviter_host, inviter_port))
            inviter_socket.send(
                f"find_successor {self.__finger_table[0].start}".encode("utf-8")
            )
            self.__finger_table[0].node = pickle.loads(inviter_socket.recv(10240))
            inviter_socket.send("close".encode("utf-8"))
            inviter_socket.close()
            # move keys in (predecessor, n] from successor
        else:
            self.__init_finger_table(inviter_host, inviter_port)
            self.__update_others()
            self.__stabilize_thread.start()
            self.__fix_fingers_thread.start()

    def __init_finger_table(self, inviter_host: str, inviter_port: int):
        inviter_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        inviter_socket.connect((inviter_host, inviter_port))
        inviter_socket.send(
            f"find_successor {self.__finger_table[0].start}".encode("utf-8")
        )
        self.__finger_table[0].node = pickle.loads(inviter_socket.recv(10240))
        for i in range(0, Node.hash_size - 1):
            if self.__finger_table[i + 1].start in range(
                self.id, self.__finger_table[i].node.id
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
            if p.id is not self.id:
                p_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                p_socket.connect((p.host, p.port))
                p_socket.send(f"update_finger_table {self.id} {i}".encode("utf-8"))
                _ = p_socket.recv(1024)
                p_socket.send("close".encode("utf-8"))
                p_socket.close()

    def __update_finger_table(self, id: int, i: int):
        if id in range(self.id, self.__finger_table[i].node.id):
            self.__finger_table[i].node = id
            p_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            p_socket.connect((self.__predecessor.host, self.__predecessor.port))
            p_socket.send(f"update_finger_table {self.id} {i}".encode("utf-8"))
            _ = p_socket.recv(1024)
            p_socket.send("close".encode("utf-8"))
            p_socket.close()

    def __stabilize(self):
        while self.__active:
            s_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s_socket.connect(
                (self.__finger_table[0].node.host, self.__finger_table[0].node.port)
            )
            s_socket.send("get_your_predecessor".encode("utf-8"))
            successors_predecessor = pickle.loads(s_socket.recv(10240))
            s_socket.send("close".encode("utf-8"))
            s_socket.close()
            if successors_predecessor not in range(
                self.id + 1, self.__finger_table[0].node.id
            ):  # successors_predecessor not in (self.id, successor.id), thus [self.id+1, successor.id)
                self.__finger_table[0].node = successors_predecessor
            s_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s_socket.connect(
                (self.__finger_table[0].node.host, self.__finger_table[0].node.port)
            )
            s_socket.send(f"notify {self.id} {self.host} {self.port}".encode("utf-8"))
            _ = s_socket.recv(1024)
            s_socket.send("close".encode("utf-8"))
            s_socket.close()
            time.sleep(self.__stabilize_interval)

    def __notify(self, id: int, host: str, port: int):
        if self.__predecessor is self.id or id in range(self.__predecessor.id, self.id):
            self.__predecessor = FingerNodeInfo(id, host, port)

    def __fix_fingers(self):
        while self.__active:
            i = random.randint(0, 127)
            self.__finger_table[i].node = self.__find_successor(
                self.__finger_table[i].start
            )
            time.sleep(self.__fix_fingers_interval)
