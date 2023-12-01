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
        for i in range(settings.size_successor_list):
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
                case "print":
                    print(command[1])
                    peer_socket.send("done".encode("utf-8"))
                case "ping":
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
                    peer_socket.sendall(pickle.dumps(self.__get_successor()))
                case "get_your_predecessor":
                    peer_socket.sendall(pickle.dumps(self.__predecessor))
                case "initialize_network":
                    self.__initialize_network()
                    peer_socket.send("done".encode("utf-8"))
                case "join":
                    self.__join(command[1], int(command[2]))
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
                case "get_successor_list":
                    if len(command) > 1:
                        print(self.__successor_list[int(command[1])])
                    else:
                        for entry in self.__successor_list:
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
            successor = self.__get_successor()
        return successor

    def __find_predecessor(self, id: int):
        # You are the predecessor
        if self.__circular_range(
            id, self.id + 1, self.__get_successor().id + 1
        ):  # id not in (self.id, self.successor], [self.id+1, self.successor+1)
            return NodeInfo(self.id, self.host, self.port)
        # You are not the predecessor
        n = self.__closest_preceeding_finger(id)
        if n.id != self.id:
            while True:
                successor = send_command_with_response(
                    "get_your_successor", n.host, n.port
                )
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
        self.__successor_list[0] = send_command_with_response(
            f"find_successor {self.__finger_table[0].start}",
            inviter_host,
            inviter_port,
        )
        self.__stabilize_thread.start()
        self.__fix_fingers_thread.start()
        # move values in (predecessor, n] from successor

    def __stabilize(self):
        while self.__active:
            successor = self.__get_successor()
            if successor.id != self.id:
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
                self.__successor_list[0] = successor
            for i in range(1, len(self.__successor_list)):
                while True:
                    try:
                        self.__successor_list[i] = send_command_with_response(
                            "get_your_successor",
                            self.__successor_list[i - 1].host,
                            self.__successor_list[i - 1].port,
                        )
                        break
                    except Exception:
                        continue
                        # print(
                        #    f"{e}: Node {self.id} accessing self.__successor_list[{i-1}], which is {self.__successor_list[i-1]}"
                        # )
                        # print(f"{e}: Node {self.id} finger_list:")
                        # for entry in self.__successor_list:
                        #    print(entry)
                        # print(f"{e}: Successor was {successor}")

            if self.id != successor.id:
                send_command(
                    f"notify {self.id} {self.host} {self.port}",
                    successor.host,
                    successor.port,
                )
            time.sleep(self.__stabilize_interval)

    def __notify(self, id: int, host: str, port: int):
        if self.__circular_range(id, self.__predecessor.id, self.id):
            self.__predecessor = NodeInfo(id, host, port)
        else:
            try:
                send_command("ping", self.__predecessor.host, self.__predecessor.port)
            except Exception:
                self.__predecessor = NodeInfo(id, host, port)

    def __fix_fingers(self):
        while self.__active:
            i = random.randint(0, 127)
            self.__finger_table[i].node = self.__find_successor(
                self.__finger_table[i].start
            )
            time.sleep(self.__fix_fingers_interval)

    def __get_successor(self):
        for entry in self.__successor_list:
            if entry.id != self.id:
                try:
                    send_command("ping", entry.host, entry.port)
                    return entry
                except Exception:
                    pass
        return NodeInfo(self.id, self.host, self.port)

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
