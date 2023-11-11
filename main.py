import multiprocessing
import socket
import time

from node.node import Node

num_nodes = 2
processes = []
for i in range(num_nodes):
    node = Node("localhost", 8000 + i)
    processes.append(multiprocessing.Process(target=node.start_node))
    processes[-1].start()


# Placeholder from now on
# Info for node1
node1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
node1_ip = "127.0.0.1"
node1_port = 8000

# Info for node2
node2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
node2_ip = "127.0.0.1"
node2_port = 8001

time.sleep(1)

# Test communication with node1
node1.connect((node1_ip, node1_port))
try:
    print(f"Connecting to {node1_ip}:{node1_port}")
    while True:
        msg = input("Enter command: ")
        node1.send(msg.encode("utf-8")[:1024])
        response = node1.recv(1024).decode("utf-8")
        if response == "close":
            node1.close()
            print("Closing connection")
            break
        elif response == "invalid":
            print("Invalid Command")
except Exception as e:
    print(f"Error: {e}")

# Test communication with node2
node2.connect((node2_ip, node2_port))
try:
    print(f"Connecting to {node2_ip}:{node2_port}")
    while True:
        msg = input("Enter command: ")
        node2.send(msg.encode("utf-8")[:1024])
        response = node2.recv(1024).decode("utf-8")
        if response == "close":
            node2.close()
            print("Closing connection")
            break
        elif response == "invalid":
            print("Invalid Command")
except Exception as e:
    print(f"Error: {e}")
