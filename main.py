import hashlib
import multiprocessing
import random
import socket
import time

import pandas as pd

from node.chord import ChordNode, ChordNodeSettings
from node.console import console
from node.request import send_lookup_command, send_store_command

df = pd.read_csv("dataset/list_of_computer_scientists.csv")
data = {}
for institution, df_group in df.groupby(["Institution"]):
    people = []
    for row_index, row in df_group.iterrows():
        people.append({"name": row["Name"], "awards": row["Awards"]})
    data[institution[0]] = people  # type: ignore

num_nodes = 40
size_successor_list = 5
processes = []
base_port = random.randint(8000, 10000)
for i in range(num_nodes):
    node = ChordNode(
        host="localhost",
        port=base_port + i,
        settings=ChordNodeSettings(size_successor_list, 0.5, 0.3, 0.2),
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

print("Waiting 20 Seconds For Nodes to Synchronize\n")
time.sleep(20)

i = 1
for institution in data.keys():
    print(
        f"\rInserting Institution Data {i}/{len(data)}",
        end="",
        flush=True,
    )
    for person in data[institution]:
        send_store_command(
            "localhost",
            base_port,
            int(hashlib.md5((institution).encode()).hexdigest(), 16),
            str(person["awards"]),
            str(person["name"]),
        )
    i += 1
print("\n")

console(processes, True)
