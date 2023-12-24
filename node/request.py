import pickle
import random
import socket
import time


def send_command(command: str, host: str, port: int):
    # print(f"{host}:{port}: Sending message", flush=True)
    comm_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    comm_socket.settimeout(2.0)
    comm_socket.connect((host, port))
    comm_socket.send(command.encode("utf-8"))
    _ = comm_socket.recv(1024)
    comm_socket.send("close".encode("utf-8"))
    _ = comm_socket.recv(1024)
    comm_socket.close()


def send_command_async(command: str, host: str, port: int):
    # print(f"{host}:{port}: Sending message", flush=True)
    comm_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    comm_socket.settimeout(2.0)
    comm_socket.connect((host, port))
    comm_socket.send(command.encode("utf-8"))
    _ = comm_socket.recv(1024)
    comm_socket.close()


def send_command_with_response(command: str, host: str, port: int):
    # print(f"{host}:{port}: Sending message", flush=True)
    comm_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    comm_socket.settimeout(5.0)
    comm_socket.connect((host, port))
    comm_socket.send(command.encode("utf-8"))
    response = pickle.loads(comm_socket.recv(1024))
    comm_socket.send("close".encode("utf-8"))
    _ = comm_socket.recv(1024)
    comm_socket.close()
    return response


def send_store_command(host: str, port: int, chord_key: int, data_key: str, data):
    # print(f"{host}:{port}: Sending message", flush=True)
    if data_key != "nan":
        data_key = str(float(data_key))
    comm_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    comm_socket.settimeout(5.0)
    comm_socket.connect((host, port))
    comm_socket.send("store".encode("utf-8"))
    _ = comm_socket.recv(1024)
    comm_socket.sendall(pickle.dumps(chord_key))
    _ = comm_socket.recv(1024)
    comm_socket.sendall(pickle.dumps(data_key))
    _ = comm_socket.recv(1024)
    comm_socket.sendall(pickle.dumps(data))
    _ = comm_socket.recv(1024)
    comm_socket.sendall("close".encode("utf-8"))
    _ = comm_socket.recv(1024)
    comm_socket.close()


def send_transfer_receive_command(
    host: str, port: int, chord_key: int, data_key: str, data
):
    # print(f"{host}:{port}: Sending message", flush=True)
    comm_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    comm_socket.settimeout(5.0)
    comm_socket.connect((host, port))
    comm_socket.send("transfer_receive".encode("utf-8"))
    _ = comm_socket.recv(1024)
    comm_socket.sendall(pickle.dumps(chord_key))
    _ = comm_socket.recv(1024)
    comm_socket.sendall(pickle.dumps(data_key))
    _ = comm_socket.recv(1024)
    comm_socket.sendall(pickle.dumps(data))
    _ = comm_socket.recv(1024)
    comm_socket.sendall("close".encode("utf-8"))
    _ = comm_socket.recv(1024)
    comm_socket.close()


def send_lookup_command(host: str, port: int, chord_key: int, data_key: str):
    # print(f"{host}:{port}: Sending message", flush=True)
    if data_key != "nan":
        data_key = str(float(data_key))
    comm_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    comm_socket.settimeout(15.0)
    comm_socket.connect((host, port))
    comm_socket.send("lookup".encode("utf-8"))
    _ = comm_socket.recv(1024)
    comm_socket.sendall(pickle.dumps(chord_key))
    _ = comm_socket.recv(1024)
    comm_socket.sendall(pickle.dumps(data_key))
    _ = comm_socket.recv(1024)
    result = pickle.loads(comm_socket.recv(10240))
    comm_socket.sendall("close".encode("utf-8"))
    _ = comm_socket.recv(1024)
    comm_socket.close()
    return result
