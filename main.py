import multiprocessing
import random
import socket
import time

import pandas as pd

from node.chord import ChordNode, FingerUpdateSettings

df = pd.read_csv("dataset/list_of_computer_scientists.csv")
data = {}
for institution, df_group in df.groupby(["Institution"]):
    people = []
    for row_index, row in df_group.iterrows():
        people.append([row["Name"], row["Awards"]])
    data[institution[0]] = people

num_nodes = 30
processes = []
base_port = random.randint(8000, 10000)
for i in range(num_nodes):
    node = ChordNode(
        host="localhost",
        port=base_port + i,
        finger_update_settings=FingerUpdateSettings("aggressive", None, None),
    )
    processes.append(multiprocessing.Process(target=node.start_node))
    processes[-1].start()
    time.sleep(1)
    node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    node_socket.connect(("localhost", base_port + i))
    if i == 0:
        node_socket.send("initialize_network".encode("utf-8"))
        _ = node_socket.recv(1024)
        node_socket.send("close".encode("utf-8"))
    else:
        node_socket.send(f"join localhost {base_port}".encode("utf-8"))
        _ = node_socket.recv(1024)
        node_socket.send("close".encode("utf-8"))
    node_socket.close()

# Placeholder from now on
# Test communication with nodes
for i in range(num_nodes):
    node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    node_socket.connect(("localhost", base_port + i))
    try:
        print(f"Connecting to localhost:{base_port+i}")
        while True:
            msg = input("Enter command: ")
            node_socket.send(msg.encode("utf-8")[:1024])
            response = node_socket.recv(1024).decode("utf-8")
            if response == "close":
                node_socket.close()
                print("Closing connection")
                break
            elif response == "invalid":
                print("Invalid Command")
    except Exception as e:
        print(f"Error: {e}")
