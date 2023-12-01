import pickle
import random
import socket
import time


def send_command(command: str, host: str, port: int):
    # print(f"Command: {command}, Host: {host}, Port {port}")
    while True:  # To handle deadlocks if two communications happen at the same time
        try:
            print(f"Command: {command}, Host: {host}, Port {port}")
            comm_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            comm_socket.settimeout(2.0)
            comm_socket.connect((host, port))
            # print("are we deadlocked")
            comm_socket.send(command.encode("utf-8"))
            _ = comm_socket.recv(1024)
            # print("no")
            # print("are we deadlocked")
            comm_socket.send("close".encode("utf-8"))
            _ = comm_socket.recv(1024)
            # print("no")
            # print(f"Exit Command: {command}, Host: {host}, Port {port}")
            comm_socket.close()
            return
        except Exception:
            print(f"Timeout reached with command {host}:{port} {command}")
            time.sleep(random.uniform(1.0, 3.0))


def send_command_with_response(command: str, host: str, port: int):
    while True:
        try:
            print(f"Command: {command}, Host: {host}, Port {port}")
            comm_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            comm_socket.settimeout(5.0)
            comm_socket.connect((host, port))
            # print("are we deadlocked")
            comm_socket.send(command.encode("utf-8"))
            response = pickle.loads(comm_socket.recv(1024))
            # print("no")
            # print("are we deadlocked")
            comm_socket.send("close".encode("utf-8"))
            _ = comm_socket.recv(1024)
            # print("no")
            comm_socket.close()
            # print(f"Exit Command: {command}, Host: {host}, Port {port}")
            return response
        except Exception:
            print(f"Timeout reached with command {host}:{port} {command}")
            time.sleep(random.uniform(1.0, 3.0))
