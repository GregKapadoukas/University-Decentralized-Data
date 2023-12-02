import hashlib
import multiprocessing
import random
import socket
import time

import pandas as pd

from node.chord import ChordNode, ChordNodeSettings
from node.console import console
from node.request import send_command, send_lookup_command, send_store_command

df = pd.read_csv("dataset/list_of_computer_scientists.csv")
data = {}
for institution, df_group in df.groupby(["Institution"]):
    people = []
    for row_index, row in df_group.iterrows():
        people.append([row["Name"], row["Awards"]])
    data[institution[0]] = people  # type: ignore

num_nodes = 5
size_successor_list = 5
processes = []
base_port = random.randint(8000, 10000)
for i in range(1):
    node = ChordNode(
        host="localhost",
        port=base_port + i,
        settings=ChordNodeSettings(size_successor_list, 1, 0.05, 0.05),
    )
    processes.append(multiprocessing.Process(target=node.start_node))
    processes[-1].start()
    time.sleep(0.01)
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
print("Waiting for nodes to synchronize")
time.sleep(5)

print(
    f"Storing first entry with hash {int(hashlib.md5(('CEID').encode()).hexdigest(), 16)}"
)
send_store_command(
    "localhost",
    base_port,
    int(hashlib.md5(("CEID").encode()).hexdigest(), 16),
    "15",
    "Gregory Kapadoukas",
)

print(
    f"Storing second entry with hash {int(hashlib.md5(('CEID').encode()).hexdigest(), 16)}"
)
send_store_command(
    "localhost",
    base_port,
    int(hashlib.md5(("CEID").encode()).hexdigest(), 16),
    "15",
    "Kostas Kapogiannis",
)

result = send_lookup_command(
    "localhost", base_port, int(hashlib.md5(("CEID").encode()).hexdigest(), 16), "15"
)
print(f"Result for Institution: CEID and #Awards: 15 is: {result}")

console(processes, False)

for i in range(1, num_nodes):
    node = ChordNode(
        host="localhost",
        port=base_port + i,
        settings=ChordNodeSettings(size_successor_list, 1, 0.05, 0.05),
    )
    processes.append(multiprocessing.Process(target=node.start_node))
    processes[-1].start()
    time.sleep(0.1)
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

# Console
console(processes, False)
result = send_lookup_command(
    "localhost", base_port, int(hashlib.md5(("CEID").encode()).hexdigest(), 16), "15"
)
print(f"Result for Institution: CEID and #Awards: 15 is: {result}")
console(processes, True)
