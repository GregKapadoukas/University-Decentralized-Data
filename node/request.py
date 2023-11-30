import pickle
import socket


def send_command(command: str, host: str, port: int):
    # print(f"Command: {command}, Host: {host}, Port {port}")
    comm_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    comm_socket.connect((host, port))
    comm_socket.send(command.encode("utf-8"))
    _ = comm_socket.recv(1024)
    comm_socket.send("close".encode("utf-8"))
    _ = comm_socket.recv(1024)
    # print(f"Exit Command: {command}, Host: {host}, Port {port}")
    comm_socket.close()


def send_command_with_response(command: str, host: str, port: int):
    # print(f"Command: {command}, Host: {host}, Port {port}")
    comm_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    comm_socket.connect((host, port))
    comm_socket.send(command.encode("utf-8"))
    response = pickle.loads(comm_socket.recv(1024))
    comm_socket.send("close".encode("utf-8"))
    _ = comm_socket.recv(1024)
    comm_socket.close()
    # print(f"Exit Command: {command}, Host: {host}, Port {port}")
    return response
