import multiprocessing
import random
import socket
import time

import pandas as pd

from node.chord import ChordNode, FingerUpdateSettings
from node.request import send_command

df = pd.read_csv("dataset/list_of_computer_scientists.csv")
data = {}
for institution, df_group in df.groupby(["Institution"]):
    people = []
    for row_index, row in df_group.iterrows():
        people.append([row["Name"], row["Awards"]])
    data[institution[0]] = people  # type: ignore

num_nodes = 3
processes = []
base_port = random.randint(8000, 10000)
for i in range(num_nodes):
    node = ChordNode(
        host="localhost",
        port=base_port + i,
        finger_update_settings=FingerUpdateSettings("normal", 3, 0.05),
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
print('Enter command in the following format "node_host" node_port" "command"')
while True:
    msg = input("# ")
    if msg == "exit":
        for process in processes:
            process.terminate()
            process.join()
        break
    msg = msg.split(" ", 2)
    if len(msg) == 3:
        send_command(str(msg[2]), str(msg[0]), int(msg[1]))
    else:
        print("Invalid Command")
