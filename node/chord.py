import pickle
import random
import threading
import time

from node.node import Node
from node.request import send_command, send_command_with_response


class ChordNodeSettings:
    def __init__(self, size_successor_list, stabilize_interval, fix_fingers_interval):
        self.size_successor_list = size_successor_list
        self.stabilize_interval = stabilize_interval
        self.fix_fingers_interval = fix_fingers_interval


class NodeInfo:
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
        self.node = NodeInfo(id, host, port)

    def __str__(self):
        return f"start: {self.start}, interval {self.interval}, node {self.node}"


class ChordNode(Node):
    def __init__(
        self,
        host,
        port,
        settings,
    ):
        super().__init__(host, port)
        self.__predecessor = NodeInfo(self.id, self.host, self.port)
        self.__finger_table = []
        i = 1
        for i in range(1, Node.hash_size + 1):  # 16*8 because MD5 has is 16 bytes
            self.__finger_table.append(
                FingerEntry(
                    start=(self.id + (2 ** (i - 1))) % Node.hash_max_num,
                    interval=range(
                        (self.id + (2 ** (i - 1))) % Node.hash_max_num,
                        (self.id + (2 ** (i))) % Node.hash_max_num,
                    ),
                    id=self.id,
                    host=self.host,
                    port=self.port,
                )
            )
        self.__successor_list = []
        for i in range(i, settings.size_successor_list):
            self.__successor_list.append(NodeInfo(self.id, self.host, self.port))
        self.__stabilize_interval = settings.stabilize_interval
        self.__fix_fingers_interval = settings.fix_fingers_interval
        self.__stabilize_thread = threading.Thread(target=self.__stabilize)
        self.__fix_fingers_thread = threading.Thread(target=self.__fix_fingers)
        self.__active = True

    def handleCommands(self, peer_socket):
        while True:
            # print(f"{self.id}: Now listening for commands")
            command = peer_socket.recv(1024).decode("utf-8").split()
            # print(command)
            match command[0]:
                case "leave":
                    # Add communication to successor and __predecessor
                    # Inform requester
                    self.__active = False
                    self.__stabilize_thread.join()
                    self.__fix_fingers_thread.join()
                    self.__leave()
                    peer_socket.send("done".encode("utf-8"))
                    peer_socket.close()
                    return "close"
                case "set_predecessor":
                    self.__predecessor = NodeInfo(
                        int(command[1]), str(command[2]), int(command[3])
                    )
                    peer_socket.send("done".encode("utf-8"))
                case "print":
                    print(command[1])
                    peer_socket.send("done".encode("utf-8"))
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
                    peer_socket.send("done".encode("utf-8"))
                case "join":
                    self.__join(command[1], int(command[2]))
                    peer_socket.send("done".encode("utf-8"))
                case "update_finger_table":
                    self.__update_finger_table(
                        int(command[1]),
                        str(command[2]),
                        int(command[3]),
                        int(command[4]),
                    )
                    peer_socket.send("done".encode("utf-8"))
                case "notify":
                    # print("Got command")
                    self.__notify(int(command[1]), str(command[2]), int(command[3]))
                    peer_socket.send("done".encode("utf-8"))
                case "close":
                    peer_socket.send("close".encode("utf-8"))
                    peer_socket.close()
                    # print(f"{self.id}: Closing socket")
                    return "continue"
                # For debugging
                case "get_self":
                    print(NodeInfo(self.id, self.host, self.port))
                    peer_socket.send("done".encode("utf-8"))
                case "get_finger_table":
                    if len(command) > 1:
                        print(self.__finger_table[int(command[1])])
                    else:
                        for entry in self.__finger_table:
                            print(entry)
                    peer_socket.send("done".encode("utf-8"))
                case "get_predecessor":
                    print(self.__predecessor)
                    peer_socket.send("done".encode("utf-8"))
                case _:
                    peer_socket.send("invalid".encode("utf-8"))

    def __find_successor(self, id: int):
        n = self.__find_predecessor(id)
        # If you are not the successor
        if n.id != self.id:
            successor = send_command_with_response(
                f"get_your_successor {id}", n.host, n.port
            )
        # If you are the successor
        else:
            # print("Inviter: I am the predecessor, take my successor")
            successor = self.__finger_table[0].node
        return successor

    def __find_predecessor(self, id: int):
        # You are the predecessor
        if self.__circular_range(
            id, self.id + 1, self.__finger_table[0].node.id + 1
        ):  # id not in (self.id, self.successor], [self.id+1, self.successor+1)
            return NodeInfo(self.id, self.host, self.port)
        # You are not the predecessor
        n = self.__closest_preceeding_finger(id)
        # print(f"My closest finger is: {n} (obviously 1)")
        if n.id != self.id:
            while True:
                successor = send_command_with_response(
                    "get_your_successor", n.host, n.port
                )
                # print(
                #    f"value is: {id}, which should be in range  ({n.id}, {successor.id}]"
                # )
                if self.__circular_range(
                    id, n.id + 1, successor.id + 1
                ):  # id in (n.id, successor]
                    break
                n = send_command_with_response(
                    f"closest_preceeding_finger {id}", n.host, n.port
                )
        return n

    def __closest_preceeding_finger(self, id: int):
        for i in range(Node.hash_size - 1, -1, -1):
            if self.__circular_range(
                self.__finger_table[i].node.id, self.id + 1, id
            ):  # node.id in (self.id, id), thus [self.id+1, id)
                return self.__finger_table[i].node
        return NodeInfo(self.id, self.host, self.port)

    def __initialize_network(self):
        n = NodeInfo(self.id, self.host, self.port)
        for i in range(0, Node.hash_size):
            self.__finger_table[i].node = n
        self.__predecessor = n
        self.__stabilize_thread.start()
        self.__fix_fingers_thread.start()

    def __join(self, inviter_host: str, inviter_port: int):
        self.__predecessor = NodeInfo(self.id, self.host, self.port)
        # print(f"Setting my predecessor to: {self.__predecessor}")
        # self.__predecessor = None
        self.__finger_table[0].node = send_command_with_response(
            f"find_successor {self.__finger_table[0].start}",
            inviter_host,
            inviter_port,
        )
        # print(f"Setting my successor to: {self.__finger_table[0].node}")
        self.__stabilize_thread.start()
        self.__fix_fingers_thread.start()
        # move values in (predecessor, n] from successor

    def __update_finger_table(self, id: int, host: str, port: int, i: int):
        # if circular_range(id, self.id, self.__finger_table[i].node.id):
        if self.__circular_range(
            id, self.__finger_table[i].start, self.__finger_table[i].node.id
        ):
            # print(f"Updating node: {self.id} finger tables with me: {id}")
            self.__finger_table[i].node = NodeInfo(id, host, port)
            if self.__predecessor.id != self.__finger_table[i].node.id:
                send_command(
                    f"update_finger_table {id} {host} {port} {i}",
                    self.__predecessor.host,
                    self.__predecessor.port,
                )

    def __stabilize(self):
        while self.__active:
            # print(f"{self.id}: Stabilizing")
            successor, finger_position = self.__get_successor()
            try:
                # print(f"{self.id}: Stabilizing with successor {successor} in position {finger_position}")
                if finger_position != -1:
                    # print(f"{self.id}: Got out")
                    successors_predecessor = send_command_with_response(
                        "get_your_predecessor", successor.host, successor.port
                    )
                else:
                    successors_predecessor = self.__predecessor
                if self.__circular_range(
                    successors_predecessor.id, self.id + 1, successor.id
                ):  # successors_predecessor not in (self.id, successor.id), thus [self.id+1, successor.id)
                    successor = successors_predecessor
                    self.__finger_table[0].node = successor
                    # print(f"{self.id}: Updating successor to {self.__finger_table[0].node}")
                if self.id != successor.id:
                    # print(f"{self.id}: Notifying {successor}")
                    send_command(
                        f"notify {self.id} {self.host} {self.port}",
                        successor.host,
                        successor.port,
                    )
                time.sleep(self.__stabilize_interval)
            except Exception:
                self.__finger_table[finger_position].node = NodeInfo(
                    self.id, self.host, self.port
                )
                print(f"{self.id}: Stabilize: Detected unexpected node disconnection")

    def __notify(self, id: int, host: str, port: int):
        # print("Entered notify")
        if self.__circular_range(id, self.__predecessor.id, self.id):
            self.__predecessor = NodeInfo(id, host, port)
            # print(f"{self.id}: Updating predecessor to {self.__predecessor}")
        # print("Exited notify")

    def __fix_fingers(self):
        while self.__active:
            # print(f"{self.id}: Fixing fingers")
            # print(f"{self.id}: Fix fingers")
            i = random.randint(0, 127)
            self.__finger_table[i].node = self.__find_successor(
                self.__finger_table[i].start
            )
            # print(f"{self.id}: Updating finger {i} to {self.__finger_table[i].node}")
            time.sleep(self.__fix_fingers_interval)

    def __get_successor(self):
        i = 0
        for entry in self.__finger_table:
            if entry.node.id != self.id:
                return entry.node, i
            i += 1
        return NodeInfo(self.id, self.host, self.port), -1

    def __leave(self):
        # Send data to predecessor
        pass

    def __circular_range(self, value, start, end):
        if start < end:
            # Normal range, no wrap-around
            return start <= value < end
        elif start == end:
            return True
        else:
            # Range wraps around the maximum value
            return start <= value or value < end
