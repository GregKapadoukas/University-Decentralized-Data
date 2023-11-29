import pickle
import random
import socket
import threading
import time

from numpy import who

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
                    start=(self.id + (2 ** (i - 1))) % Node.hash_max_num,
                    interval=range(
                        (self.id + (2 ** (i - 1))) % Node.hash_max_num,
                        (self.id + (2 ** (i))) % Node.hash_max_num,
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
                    predecessor = self.__find_predecessor(int(command[1]))
                    peer_socket.sendall(pickle.dumps(predecessor))
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
                    self.__update_finger_table(
                        int(command[1]),
                        str(command[2]),
                        int(command[3]),
                        int(command[4]),
                    )
                    peer_socket.send("keep".encode("utf-8"))
                case "notify":
                    self.__notify(int(command[1]), str(command[2]), int(command[3]))
                    peer_socket.send("keep".encode("utf-8"))
                case "close":
                    peer_socket.close()
                    return "continue"
                # For debugging
                case "get_self":
                    print(FingerNodeInfo(self.id, self.host, self.port))
                    peer_socket.send("keep".encode("utf-8"))
                case "get_finger_table":
                    for entry in self.__finger_table:
                        print(entry)
                    peer_socket.send("keep".encode("utf-8"))
                case "get_predecessor":
                    print(self.__predecessor)
                    peer_socket.send("keep".encode("utf-8"))
                case _:
                    peer_socket.send("invalid".encode("utf-8"))

    def __find_successor(self, id: int):
        n = self.__find_predecessor(id)
        # If you are not the successor
        if n.id != self.id:
            n_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            n_socket.connect((n.host, n.port))
            n_socket.send(f"get_your_successor {id}".encode("utf-8"))
            successor = pickle.loads(n_socket.recv(1024))
            n_socket.send("close".encode("utf-8"))
            n_socket.close()
        # If you are the successor
        else:
            print("Inviter: I am the predecessor, take my successor")
            successor = self.__finger_table[0].node
        return successor

    def __find_predecessor(self, id: int):
        # You are the predecessor
        if circular_range(
            id, self.id + 1, self.__finger_table[0].node.id + 1
        ):  # id not in (self.id, self.successor], [self.id+1, self.successor+1)
            return FingerNodeInfo(self.id, self.host, self.port)
        # You are not the predecessor
        n = self.__closest_preceeding_finger(id)
        # print(f"My closest finger is: {n} (obviously 1)")
        if n.id != self.id:
            while True:
                n_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                n_socket.connect((n.host, n.port))
                n_socket.send("get_your_successor".encode("utf-8"))
                successor = pickle.loads(n_socket.recv(1024))
                # print(
                #    f"value is: {id}, which sould be in range  ({n.id}, {successor.id}]"
                # )
                if circular_range(
                    id, n.id + 1, successor.id + 1
                ):  # id in (n.id, sucessor]
                    n_socket.send("close".encode("utf-8"))
                    n_socket.close()
                    break
                n_socket.send(f"closest_preceeding_finger {id}".encode("utf-8"))
                n = pickle.loads(n_socket.recv(1024))
                n_socket.send("close".encode("utf-8"))
                n_socket.close()
        return n

    def __closest_preceeding_finger(self, id: int):
        for i in range(Node.hash_size - 1, -1, -1):
            if circular_range(
                self.__finger_table[i].node.id, self.id + 1, id
            ):  # node.id in (self.id, id), thus [self.id+1, id)
                return self.__finger_table[i].node
        return FingerNodeInfo(self.id, self.host, self.port)

    def __initialize_network(self):
        n = FingerNodeInfo(self.id, self.host, self.port)
        for i in range(0, Node.hash_size):
            self.__finger_table[i].node = n
        self.__predecessor = n
        if self.__finger_update_mode == "normal":
            self.__stabilize_thread.start()
            self.__fix_fingers_thread.start()

    def __join(self, inviter_host: str, inviter_port: int):
        if self.__finger_update_mode == "aggressive":
            self.__init_finger_table(inviter_host, inviter_port)
            self.__update_others()
            # move keys in (predecessor, n] from successor
        else:
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
            self.__stabilize_thread.start()
            self.__fix_fingers_thread.start()

    def __init_finger_table(self, inviter_host: str, inviter_port: int):
        # Get your successor
        inviter_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        inviter_socket.connect((inviter_host, inviter_port))
        print(f"Looking for successor of: {self.__finger_table[0].start}")
        inviter_socket.send(
            f"find_successor {self.__finger_table[0].start}".encode("utf-8")
        )
        self.__finger_table[0].node = pickle.loads(inviter_socket.recv(10240))
        inviter_socket.send("close".encode("utf-8"))
        inviter_socket.close()
        print(f"My successor is: {self.__finger_table[0].node.id}")

        # Get your predecessor
        successor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        successor_socket.connect(
            (self.__finger_table[0].node.host, self.__finger_table[0].node.port)
        )
        successor_socket.send("get_your_predecessor".encode("utf-8"))
        self.__predecessor = pickle.loads(successor_socket.recv(10240))
        successor_socket.send("close".encode("utf-8"))
        successor_socket.close()
        print(f"My predecessor is {self.__predecessor.id}")

        # Tell your successor to make you predecessor
        successor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        successor_socket.connect(
            (self.__finger_table[0].node.host, self.__finger_table[0].node.port)
        )
        successor_socket.send(
            f"notify {self.id} {self.host} {self.port}".encode("utf-8")
        )
        _ = successor_socket.recv(10240)
        successor_socket.send("close".encode("utf-8"))
        successor_socket.close()

        # Get your finger table
        for i in range(0, Node.hash_size - 1):
            if circular_range(
                self.__finger_table[i + 1].start,
                self.id,
                self.__finger_table[i].node.id,
            ):
                self.__finger_table[i + 1].node = self.__finger_table[i].node
                # print(
                #    f"Updating finger_table[{i+1}] to: {self.__finger_table[i+1].node.id}"
                # )
            else:
                inviter_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                inviter_socket.connect((inviter_host, inviter_port))
                inviter_socket.send(
                    f"find_successor {self.__finger_table[i+1].start}".encode("utf-8")
                )
                self.__finger_table[i + 1].node = pickle.loads(
                    inviter_socket.recv(1024)
                )
                # print(
                #    f"Updating finger_table[{i+1}] to: {self.__finger_table[i+1].node.id}"
                # )
                inviter_socket.send("close".encode("utf-8"))
                inviter_socket.close()
        # print("Finished with init_finger_table")

    def __update_others(self):
        for i in range(1, Node.hash_size + 1):
            # print(
            #    f"Looking for node preceeding: {(self.id - 2 ** (i-1)) % Node.hash_max_num}"
            # )
            p = self.__find_predecessor((self.id - 2 ** (i - 1)) % Node.hash_max_num)
            # print(f"Found node {p.id}")
            if p.id != self.id:
                # print("It's not me")
                p_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                p_socket.connect((p.host, p.port))
                p_socket.send(
                    f"update_finger_table {self.id} {self.host} {self.port} {i-1}".encode(
                        "utf-8"
                    )
                )
                _ = p_socket.recv(1024)
                p_socket.send("close".encode("utf-8"))
                p_socket.close()
            else:
                # print("It's me")
                self.__update_finger_table(self.id, self.host, self.port, i - 1)

    def __update_finger_table(self, id: int, host: str, port: int, i: int):
        # if circular_range(id, self.id, self.__finger_table[i].node.id):
        if circular_range(
            id, self.__finger_table[i].start, self.__finger_table[i].node.id
        ):
            # print(f"Updating node: {self.id} finger tables with me: {id}")
            self.__finger_table[i].node = FingerNodeInfo(id, host, port)
            if self.__predecessor.id != self.__finger_table[i].node.id:
                p_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                p_socket.connect((self.__predecessor.host, self.__predecessor.port))
                p_socket.send(
                    f"update_finger_table {id} {host} {port} {i}".encode("utf-8")
                )
                _ = p_socket.recv(1024)
                p_socket.send("close".encode("utf-8"))
                p_socket.close()

    def __stabilize(self):
        while self.__active:
            successor, finger_position = self.__get_successor()
            if finger_position != -1:
                try:
                    s_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s_socket.connect(
                        (
                            successor.host,
                            successor.port,
                        )
                    )
                    s_socket.send("get_your_predecessor".encode("utf-8"))
                    successors_predecessor = pickle.loads(s_socket.recv(10240))
                    s_socket.send("close".encode("utf-8"))
                    s_socket.close()
                    if not circular_range(
                        successors_predecessor, self.id + 1, successor.id
                    ):  # successors_predecessor not in (self.id, successor.id), thus [self.id+1, successor.id)
                        successor = successors_predecessor
                    if self.id != successor.id:
                        s_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s_socket.connect(
                            (
                                successor.host,
                                successor.port,
                            )
                        )
                        s_socket.send(
                            f"notify {self.id} {self.host} {self.port}".encode("utf-8")
                        )
                        _ = s_socket.recv(1024)
                        s_socket.send("close".encode("utf-8"))
                        s_socket.close()
                except Exception:
                    print("Peer lost")
                    self.__finger_table[finger_position].node = FingerNodeInfo(
                        self.id, self.host, self.port
                    )
                    print("updating successor")
            time.sleep(self.__stabilize_interval)

    def __notify(self, id: int, host: str, port: int):
        if self.__predecessor is self.id or circular_range(
            id, self.__predecessor.id, self.id
        ):
            self.__predecessor = FingerNodeInfo(id, host, port)
            print(f"Updating successor's predecessor to: {self.__predecessor.id}")

    def __fix_fingers(self):
        while self.__active:
            i = random.randint(0, 127)
            self.__finger_table[i].node = self.__find_successor(
                self.__finger_table[i].start
            )
            time.sleep(self.__fix_fingers_interval)

    def __get_successor(self):
        i = 0
        for entry in self.__finger_table:
            if entry.node.id != self.id:
                return entry.node, i
            i += 1
        return FingerNodeInfo(self.id, self.host, self.port), -1


def circular_range(value, start, end):
    if start < end:
        # Normal range, no wrap-around
        return start <= value < end
    elif start == end:
        return True
    else:
        # Range wraps around the maximum value
        return start <= value or value < end
