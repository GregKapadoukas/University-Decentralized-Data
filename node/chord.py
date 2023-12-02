import hashlib
import pickle
import random
import socket
import threading
import time

from node.node import P2PNode
from node.request import (send_command, send_command_async,
                          send_command_with_response, send_store_command,
                          send_transfer_receive_command)


class ChordNodeSettings:
    def __init__(
        self,
        size_successor_list,
        stabilize_interval,
        fix_fingers_interval,
        ping_successors_interval,
    ):
        self.size_successor_list = size_successor_list
        self.stabilize_interval = stabilize_interval
        self.fix_fingers_interval = fix_fingers_interval
        self.ping_successors_interval = ping_successors_interval


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


class ChordNode(P2PNode):
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
        for i in range(1, P2PNode.hash_size + 1):  # 16*8 because MD5 has is 16 bytes
            self.__finger_table.append(
                FingerEntry(
                    start=(self.id + (2 ** (i - 1))) % P2PNode.hash_max_num,
                    interval=range(
                        (self.id + (2 ** (i - 1))) % P2PNode.hash_max_num,
                        (self.id + (2 ** (i))) % P2PNode.hash_max_num,
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
        self.__ping_successors_interval = settings.ping_successors_interval
        self.__stabilize_thread = threading.Thread(target=self.__stabilize)
        self.__fix_fingers_thread = threading.Thread(target=self.__fix_fingers)
        self.__ping_successors_thread = threading.Thread(target=self.__ping_successors)
        self.__active = True

    def handle_commands(self, peer_connection):
        while True:
            command = peer_connection.recv(1024).decode("utf-8").split()
            match command[0]:
                case "leave":
                    # Add communication to successor and __predecessor
                    # Inform requester
                    self.__active = False
                    self.__stabilize_thread.join()
                    self.__fix_fingers_thread.join()
                    self.__ping_successors_thread.join()
                    self.__leave()
                    peer_connection.send("done".encode("utf-8"))
                    peer_connection.close()
                    return "close"
                case "print":
                    print(command[1])
                    peer_connection.send("done".encode("utf-8"))
                case "ping":
                    peer_connection.send("done".encode("utf-8"))
                case "find_successor":
                    peer_connection.send(
                        pickle.dumps(self.__find_successor(int(command[1])))
                    )
                case "find_predecessor":
                    predecessor = self.__find_predecessor(int(command[1]))
                    peer_connection.sendall(pickle.dumps(predecessor))
                case "closest_preceeding_finger":
                    node = self.__closest_preceeding_finger(int(command[1]))
                    peer_connection.sendall(pickle.dumps(node))
                case "get_your_successor":
                    peer_connection.sendall(pickle.dumps(self.__successor_list[0]))
                case "get_your_predecessor":
                    peer_connection.sendall(pickle.dumps(self.__predecessor))
                case "initialize_network":
                    self.__initialize_network()
                    peer_connection.send("done".encode("utf-8"))
                case "join":
                    self.__join(command[1], int(command[2]))
                    peer_connection.send("done".encode("utf-8"))
                case "notify":
                    self.__notify(int(command[1]), str(command[2]), int(command[3]))
                    peer_connection.send("done".encode("utf-8"))
                case "close":
                    peer_connection.send("close".encode("utf-8"))
                    peer_connection.close()
                    return "continue"
                case "store":
                    peer_connection.send("send".encode("utf-8"))
                    chord_key = pickle.loads(peer_connection.recv(1024))
                    peer_connection.send("send".encode("utf-8"))
                    data_key = pickle.loads(peer_connection.recv(1024))
                    peer_connection.send("send".encode("utf-8"))
                    data = pickle.loads(peer_connection.recv(10240))
                    self.__store(chord_key, data_key, data)
                    peer_connection.send("close".encode("utf-8"))
                case "transfer_receive":
                    peer_connection.send("send".encode("utf-8"))
                    chord_key = pickle.loads(peer_connection.recv(1024))
                    peer_connection.send("send".encode("utf-8"))
                    data_key = pickle.loads(peer_connection.recv(1024))
                    peer_connection.send("send".encode("utf-8"))
                    data = pickle.loads(peer_connection.recv(10240))
                    self.__transfer_receive(chord_key, data_key, data)
                    peer_connection.send("close".encode("utf-8"))
                case "lookup":
                    peer_connection.send("send".encode("utf-8"))
                    chord_key = pickle.loads(peer_connection.recv(1024))
                    peer_connection.send("send".encode("utf-8"))
                    data_key = pickle.loads(peer_connection.recv(1024))
                    peer_connection.send("send".encode("utf-8"))
                    result = self.__lookup(int(chord_key), str(data_key))
                    peer_connection.sendall(pickle.dumps(result))
                case "propagate_lookup":
                    peer_connection.send("done".encode("utf-8"))
                    self.__propagate_lookup(int(command[1]), str(command[2]), str(command[3]), int(command[4]))
                    peer_connection.close()
                    return "continue"
                # For debugging
                case "get_self":
                    print(NodeInfo(self.id, self.host, self.port))
                    peer_connection.send("done".encode("utf-8"))
                case "get_finger_table":
                    if len(command) > 1:
                        print(self.__finger_table[int(command[1])])
                    else:
                        for entry in self.__finger_table:
                            print(entry)
                    peer_connection.send("done".encode("utf-8"))
                case "get_successor_list":
                    if len(command) > 1:
                        print(self.__successor_list[int(command[1])])
                    else:
                        for entry in self.__successor_list:
                            print(entry)
                    peer_connection.send("done".encode("utf-8"))
                case "get_predecessor":
                    print(self.__predecessor)
                    peer_connection.send("done".encode("utf-8"))
                case _:
                    peer_connection.send("invalid".encode("utf-8"))

    def __find_successor(self, id: int):
        n = self.__find_predecessor(id)
        # If you are not the successor
        while True:
            try:
                if n.id != self.id:
                    successor = send_command_with_response(
                        f"get_your_successor {id}", n.host, n.port
                    )
                else:
                    successor = self.__successor_list[0]
                break
            except Exception:
                n = self.__find_predecessor(id)
                continue
        # If you are the successor
        return successor

    def __find_predecessor(self, id: int):
        # You are the predecessor
        if self.__circular_range(
            id, self.id + 1, self.__successor_list[0].id + 1
        ):  # id not in (self.id, self.successor], [self.id+1, self.successor+1)
            return NodeInfo(self.id, self.host, self.port)
        # You are not the predecessor
        n = self.__closest_preceeding_finger(id)
        if n.id != self.id:
            while True:
                try:
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
                except Exception:
                    self.__remove_node_from_finger_table(n.id)
                    n = self.__closest_preceeding_finger(id)
                    continue
        return n

    def __closest_preceeding_finger(self, id: int):
        for i in range(P2PNode.hash_size - 1, -1, -1):
            if self.__circular_range(
                self.__finger_table[i].node.id, self.id + 1, id
            ):  # node.id in (self.id, id), thus [self.id+1, id)
                return self.__finger_table[i].node
        return NodeInfo(self.id, self.host, self.port)

    def __initialize_network(self):
        n = NodeInfo(self.id, self.host, self.port)
        for i in range(0, P2PNode.hash_size):
            self.__finger_table[i].node = n
        self.__predecessor = n
        self.__stabilize_thread.start()
        self.__fix_fingers_thread.start()
        self.__ping_successors_thread.start()

    def __join(self, inviter_host: str, inviter_port: int):
        self.__predecessor = NodeInfo(self.id, self.host, self.port)
        try:
            self.__successor_list[0] = send_command_with_response(
                f"find_successor {self.__finger_table[0].start}",
                inviter_host,
                inviter_port,
            )
            self.__stabilize_thread.start()
            self.__fix_fingers_thread.start()
            self.__ping_successors_thread.start()
        except Exception:
            print("The inviter node can't be accessed")

    def __stabilize(self):
        while self.__active:
            exit_flag = False
            while not exit_flag:
                try:
                    successor = self.__successor_list[0]
                    if successor.id != self.id:
                        successors_predecessor = send_command_with_response(
                            "get_your_predecessor", successor.host, successor.port
                        )
                        # Edge case where successor's predecessor has left
                        try:
                            send_command(
                                "ping",
                                successors_predecessor.host,
                                successors_predecessor.port,
                            )
                        except Exception:
                            send_command(
                                f"notify {self.id} {self.host} {self.port}",
                                successor.host,
                                successor.port,
                            )
                    else:
                        successors_predecessor = self.__predecessor
                    if self.__circular_range(
                        successors_predecessor.id, self.id + 1, successor.id
                    ):  # successors_predecessor not in (self.id, successor.id), thus [self.id+1, successor.id)
                        successor = successors_predecessor

                    self.__successor_list[0] = successor
                    #Potentially transfer keys to successor (will happen when new node has joined)
                    if self.id != self.__successor_list[0].id:
                        deleted_keys = []
                        for chord_key in self.data.keys():
                            if not self.__circular_range(chord_key, self.id, self.__successor_list[0].id):
                                self.__transfer_keys(self.__successor_list[0], chord_key, self.data[chord_key])
                                deleted_keys.append(chord_key)
                        for key in deleted_keys:
                            del self.data[key]

                    for i in range(1, len(self.__successor_list)):
                        self.__successor_list[i] = send_command_with_response(
                            "get_your_successor",
                            self.__successor_list[i - 1].host,
                            self.__successor_list[i - 1].port,
                        )
                        exit_flag = True
                    if self.id != successor.id:
                        send_command(
                            f"notify {self.id} {self.host} {self.port}",
                            successor.host,
                            successor.port,
                        )
                except Exception:
                    continue
            time.sleep(self.__stabilize_interval)

    def __notify(self, id: int, host: str, port: int):
        if self.__circular_range(id, self.__predecessor.id, self.id):
            self.__predecessor = NodeInfo(id, host, port)
            #Potentially transfer keys to predecessor (will happen when new node has joined)
            if self.id != self.__predecessor.id:
                deleted_keys = []
                for chord_key in self.data.keys():
                    if self.__circular_range(chord_key, self.__predecessor.id, self.id):
                        self.__transfer_keys(self.__predecessor, chord_key, self.data[chord_key])
                        deleted_keys.append(chord_key)
                for key in deleted_keys:
                    del self.data[key]
        else:
            #Check if your old predecessor is still online, if not replace
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

    def __ping_successors(self):  # Remove nodes that left from successor list
        while self.__active:
            for i in range(len(self.__successor_list)):
                if self.__successor_list[i].id != self.id:
                    try:
                        send_command(
                            "ping",
                            self.__successor_list[i].host,
                            self.__successor_list[i].port,
                        )
                    except Exception:
                        self.__successor_list.remove(self.__successor_list[i])
                        self.__successor_list.append(
                            NodeInfo(self.id, self.host, self.port)
                        )
            time.sleep(self.__ping_successors_interval)

    def __leave(self):
        # Send data to predecessor
        for chord_key in self.data.keys():
            while True:
                if self.id != self.__predecessor.id:
                    self.__transfer_keys(self.__predecessor, chord_key, self.data[chord_key])
                    break

    def __remove_node_from_finger_table(self, id):
        for i in range(len(self.__finger_table)):
            if self.__finger_table[i].node.id == id and i > 0:
                self.__finger_table[i].node = self.__finger_table[i - 1].node
            else:
                self.__finger_table[i].node = NodeInfo(self.id, self.host, self.port)

    def __circular_range(self, value, start, end):
        if start < end:
            # Normal range, no wrap-around
            return start <= value < end
        elif start == end:
            return True
        else:
            # Range wraps around the maximum value
            return start <= value or value < end

    def __store(self, chord_key, data_key, data):
        if self.__circular_range(chord_key, self.id + 1, self.__successor_list[0].id):
            self.store_data(chord_key, data_key, data)
        else:
            while True:
                try:
                    node = self.__find_predecessor(chord_key)
                    send_store_command(node.host, node.port, chord_key, data_key, data)
                    break
                except Exception:
                    continue

    def __lookup(self, chord_key, data_key):
        if self.__circular_range(chord_key, self.id + 1, self.__successor_list[0].id):
            return self.get_data(chord_key, data_key)
        else:
            lookup_port = random.randint(10000, 12000)
            lookup_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            lookup_socket.bind((self.host, lookup_port))
            while True:
                try:
                    node = self.__find_predecessor(chord_key)
                    send_command_async(f"propagate_lookup {chord_key} {data_key} {self.host} {lookup_port}", node.host, node.port)
                    break
                except Exception:
                    continue
            lookup_socket.listen(5)
            lookup_connection, peer_addr = lookup_socket.accept()
            lookup_connection.settimeout(10.0)
            response = pickle.loads(lookup_connection.recv(10240))
            lookup_connection.send("done".encode("utf-8"))
            lookup_connection.close()
            return response

    def __propagate_lookup(self, chord_key, data_key, asker_host, asker_port):
        time.sleep(2) #Make sure that asker is ready to get response
        if self.__circular_range(chord_key, self.id + 1, self.__successor_list[0].id):
            comm_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            comm_socket.settimeout(5.0)
            comm_socket.connect((asker_host, asker_port))
            comm_socket.send(
                 pickle.dumps(self.get_data(chord_key, data_key))
            )
            _ = comm_socket.recv(1024)
            comm_socket.close()
        else:
            while True:
                try:
                    node = self.__find_predecessor(chord_key)
                    send_command(f"propagate_lookup {chord_key} {data_key} {asker_host} {asker_port}", node.host, node.port)
                    break
                except Exception:
                    continue

    def __transfer_keys(self, destination_node, chord_key, data):
        for data_key in data.keys():
            for entry in data[data_key]:
                while True:
                    try:
                        send_transfer_receive_command(destination_node.host, destination_node.port, chord_key, data_key, entry)
                        break
                    except Exception:
                        continue


    # Store but will always store on node, not propagate, useful for leave where node transfers keys but hasn't left yet
    def __transfer_receive(self, chord_key, data_key, data):
        self.store_data(chord_key, data_key, data)
